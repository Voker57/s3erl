%%%-------------------------------------------------------------------
%%% File    : s3.erl
%%% Author  : Andrew Birkett <andy@nobugs.org>
%%% Description : 
%%%
%%% Created : 14 Nov 2007 by Andrew Birkett <andy@nobugs.org>
%%%-------------------------------------------------------------------
-module(s3, [AwsCredentials]).

%% API
-export([set_acl/1,
	  list_buckets/0, create_bucket/1, delete_bucket/1,
	  list_objects/2, list_objects/1, write_object/4, read_object/2,
	  read_object/3, delete_object/2 ]).



-include_lib("xmerl/include/xmerl.hrl").
-include("../include/s3.hrl").

%%====================================================================
%% API
%%====================================================================

set_acl(ACL) ->
     put('x-amz-acl', ACL).

create_bucket (Bucket) -> {_Headers,_Body} = putRequest( AwsCredentials, Bucket, "", <<>>, "").

delete_bucket (Bucket) ->  try 
	{_Headers,_Body} = deleteRequest( AwsCredentials, Bucket, ""),
	ok
    catch
	throw:X -> X
    end.

list_buckets () -> xmlToBuckets(getRequest( AwsCredentials, "", "", [] )).

write_object (Bucket, Key, Data, ContentType) -> 
     {Headers,_Body} = putRequest(AwsCredentials,Bucket, Key, Data, ContentType),
    {value,{"ETag",ETag}} = lists:keysearch( "ETag", 1, Headers ),
    {ok, ETag}.

read_object (Bucket, Key) -> read_object (Bucket, Key, []).
read_object (Bucket, Key, Options) ->
    getRequest( AwsCredentials, Bucket, Key, [], Options).
delete_object (Bucket, Key) -> try
  {_Headers,_Body} = deleteRequest( AwsCredentials, Bucket, Key),
	ok
    catch
	throw:X -> X
    end.

%% option example: [{delimiter, "/"},{maxresults,10},{prefix,"/foo"}]
list_objects (Bucket, Options ) ->
	 Headers = lists:map( fun option_to_param/1, Options ),
    {_, Body} = getRequest( AwsCredentials, Bucket, "", Headers ),
    parseBucketListXml(Body).
 
list_objects (Bucket) -> list_objects( Bucket, [] ).


s3Host () ->
    "s3.amazonaws.com".

option_to_param( { prefix, X } ) -> 
    { "prefix", X };
option_to_param( { maxkeys, X } ) -> 
    { "max-keys", integer_to_list(X) };
option_to_param( { delimiter, X } ) -> 
    { "delimiter", X }.

getRequest( AwsCredentials, Bucket, Key, Headers) -> getRequest( AwsCredentials, Bucket, Key, Headers, [] ).
getRequest( AwsCredentials, Bucket, Key, Headers, Options ) ->
    genericRequest( AwsCredentials, get, Bucket, Key, Headers, <<>>, "", Options ).

putRequest( AwsCredentials, Bucket, Key, Content, ContentType ) ->
    genericRequest( AwsCredentials, put, Bucket, Key, [], Content, ContentType ).
deleteRequest( AwsCredentials, Bucket, Key ) ->
    genericRequest( AwsCredentials, delete, Bucket, Key, [], <<>>, "" ).


isAmzHeader( Header ) -> lists:prefix("x-amz-", Header).

canonicalizedAmzHeaders( AllHeaders ) ->
    AmzHeaders = [ {string:to_lower(K),V} || {K,V} <- AllHeaders, isAmzHeader(K) ],
    Strings = lists:map( 
		fun s3util:join/1, 
		s3util:collapse( 
		  lists:keysort(1, AmzHeaders) ) ),
    s3util:string_join( lists:map( fun (S) -> S ++ "\n" end, Strings), "").
    
canonicalizedResource ( "", "" ) -> "/";
canonicalizedResource ( Bucket, "" ) -> "/" ++ Bucket ++ "/";
canonicalizedResource ( Bucket, Path ) -> "/" ++ Bucket ++ "/" ++ Path.

stringToSign ( Verb, ContentMD5, ContentType, Date, Bucket, Path, OriginalHeaders ) ->
    Parts = [ Verb, ContentMD5, ContentType, Date, canonicalizedAmzHeaders(OriginalHeaders)],
    s3util:string_join( Parts, "\n") ++ canonicalizedResource(Bucket, Path).
    
sign (Key,Data) ->
%    io:format("Data being signed is ~p~n", [Data]),
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

queryParams( [] ) -> "";
queryParams( L ) -> 
    Stringify = fun ({K,V}) -> K ++ "=" ++ V end,
    "?" ++ s3util:string_join( lists:map( Stringify, L ), "&" ).

buildHost("") -> s3Host();
buildHost(Bucket) -> Bucket ++ "." ++ s3Host().
    
buildUrl(Bucket,Path,QueryParams) -> 
    "http://" ++ buildHost(Bucket) ++ "/" ++ Path ++ queryParams(QueryParams).

buildContentHeaders( <<>>, _ ) -> [];
buildContentHeaders( {callback, _Fun, Size}, ContentType )
   ->
    [{"Content-Length", integer_to_list(Size)},
     {"Content-Type", ContentType}];
buildContentHeaders( Contents, ContentType )
   ->
    [{"Content-Length", integer_to_list(size(Contents))},
     {"Content-Type", ContentType}].

genericRequest( AwsCredentials, Method, Bucket, Path, QueryParams, Contents, ContentType ) -> genericRequest( AwsCredentials, Method, Bucket, Path, QueryParams, Contents, ContentType, []).
genericRequest( AwsCredentials, Method, Bucket, Path, QueryParams, Contents, ContentType, Options ) ->
    Date = httpd_util:rfc1123_date(),
    MethodString = string:to_upper( atom_to_list(Method) ),
    EncodedPath = ibrowse_lib:url_encode(erlang:binary_to_list(unicode:characters_to_binary(Path))),
    Url = buildUrl(Bucket,EncodedPath,QueryParams),

    ACLHeaders = case get('x-amz-acl') of
		     ACL when is_list(ACL) -> [{"x-amz-acl", ACL}];
		     _ -> []
		 end,
    OriginalHeaders = buildContentHeaders( Contents, ContentType ) ++ ACLHeaders,
    ContentMD5 = "",
    Body = case Contents of
        {callback, Fun, _Size} -> Fun;
        Other -> Other
    end,

    #aws_credentials{ accessKeyId=AKI, secretAccessKey=SAK } = AwsCredentials,

    Signature = sign( SAK,
		      stringToSign( MethodString, ContentMD5, ContentType, 
				    Date, Bucket, EncodedPath, OriginalHeaders )),

    Headers = [ {"Authorization","AWS " ++ AKI ++ ":" ++ Signature },
		{"Host", buildHost(Bucket) },
		{"Date", Date } 
	       | OriginalHeaders ],
    HttpOptions = [ {headers_as_is,true} ] ++ Options,

%    io:format("Sending request ~p~n", [Request]),
    Reply = ibrowse:send_req(Url, Headers, Method, Body, HttpOptions, infinity),
%    io:format("HTTP reply was ~p~n", [Reply]),
    case Reply of
   {ok, Code, ResponseHeaders, ResponseBody}
      when Code=:="200"; Code=:="204"
	      -> 
	    {ResponseHeaders,ResponseBody};

	 {ok, _Code, _ResponseHeaders, ResponseBody} -> 
	    throw ( parseErrorXml(ResponseBody) )
    end.


parseBucketListXml (Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    ContentNodes = xmerl_xpath:string("/ListBucketResult/Contents", XmlDoc),

    GetObjectAttribute = fun (Node,Attribute) -> 
		      [Child] = xmerl_xpath:string( Attribute, Node ),
		      {Attribute, s3util:string_value( Child )}
	      end,

    NodeToRecord = fun (Node) ->
			   #object_info{ 
			 key =          GetObjectAttribute(Node,"Key"),
			 lastmodified = GetObjectAttribute(Node,"LastModified"),
			 etag =         GetObjectAttribute(Node,"ETag"),
			 size =         GetObjectAttribute(Node,"Size")}
		   end,
    { ok, lists:map( NodeToRecord, ContentNodes ) }.

parseErrorXml (Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    [#xmlText{value=ErrorCode}]    = xmerl_xpath:string("/Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("/Error/Message/text()", XmlDoc),
    { s3error, ErrorCode, ErrorMessage }.


xmlToBuckets( {_Headers,Body} ) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Body ),
    TextNodes       = xmerl_xpath:string("//Bucket/Name/text()", XmlDoc),
    lists:map( fun (#xmlText{value=T}) -> T end, TextNodes).


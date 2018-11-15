
-record(r_content, {
                    metadata,
                    value :: term()
                    }).

-record(r_object, {
                    bucket,
                    key,
                    contents :: [#r_content{}],
                    vclock = [],
                    updatemetadata=dict:store(clean, true, dict:new()),
                    updatevalue :: term()}).


-define(BUCKET_TYPE, <<"BucketType">>).
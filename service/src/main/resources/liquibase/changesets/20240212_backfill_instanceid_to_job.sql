-- we need a function to safely parse uuids, will be used in the next statement
create or replace function sys_wds.uuid_or_null(str text)
returns uuid as $$
begin
return str::uuid;
exception when invalid_text_representation then
  return null;
end;
$$ language plpgsql immutable;

-- set instance_id to the env var WORKSPACE_ID for pre-existing jobs where instance_id is null
-- or leave null if WORKSPACE_ID does not exist
update sys_wds.job
set instance_id = sys_wds.uuid_or_null('${WORKSPACE_ID}')
where instance_id is null;

-- if WORKSPACE_ID did not exist, or any other problems occurred, simply remove jobs
-- with null instance_id
delete from sys_wds.job where instance_id is null;

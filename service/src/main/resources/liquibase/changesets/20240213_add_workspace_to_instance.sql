-- this changeset relies on the sys_wds.uuid_or_null function, previously defined in
-- 20240213_add_workspace_to_instance.yaml

-- insert the WORKSPACE_ID env var as the workspace for all pre-existing instances
-- if WORKSPACE_ID is empty, in which case it will not parse as a uuid,
-- leave the workspace_id column null.
update sys_wds.instance
set workspace_id = sys_wds.uuid_or_null('${WORKSPACE_ID}')
where workspace_id is null;

-- for any remaining null workspace_ids, set them equal to the instance id.
-- this should never happen in data plane WDS, where $WORKSPACE_ID env var is required.
-- it should also never happen in control-plane cWDS, where we don't populate the instance table.
-- nonetheless, code this defensively to ensure that workspace_id is not null by the end of this changeset
update sys_wds.instance
set workspace_id = id
where workspace_id is null;

-- update the names for instances. If id == workspace_id, set as "default"
update sys_wds.instance
set name = 'default'
where name is null
  and id = workspace_id;

-- and if id != workspace_id, copy the id into the name
update sys_wds.instance
set name = id
where name is null;

-- finally, set the description, equal to the name
update sys_wds.instance
set description = name
where description is null;

-- Create a virtual warehouse
create warehouse snowpark_etl_wh with
    warehouse_size = 'medium'
    warehouse_type = 'standard'
    auto_suspend = 60
    auto_resume = true
    min_cluster_count = 1
    max_cluster_count = 1;

-- Create a Snowpark User
create user snowpark_user
  password = 'Password123'
  default_role = sysadmin
  default_secondary_roles = ('ALL')
  must_change_password = false;

-- Grants: Usage command not required
grant role sysadmin to user snowpark_user;
grant USAGE on warehouse snowpark_etl_wh to role sysadmin;
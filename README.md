# LinkedIn Ads Transformation dbt Package

## What does this dbt package do?
* Materializes the LinkedIn Ads RAW_main tables using the data coming from the LinkedIn API.

## How do I use the dbt package?
### Step 1: Prerequisites
To use this dby package, you must have the following:
- At least one Fivetran LinkedIn connector syncing data for at least five of the predefined reports:
    - account_history
    - creative_history
    - campaign_history
    - campaign_group_history
    - ad_analytics_by_creative
- A BigQuery data destination.

### Step 2: Install the package
Include the following LinkedIn package version in your `packages.yml` file
```yaml
packages:
  - git: "https://github.com/Incubeta/prod-fivetran-linkedin-dbt-package.git"
    revision: main
```

### Step 3: Define input tables variables
This package reads the LinkedIn data from the different tables created by the LinkedIn ads connector. 
The names of the tables can be changed by setting the correct name in the root `dbt_project.yml` file.

The following table shows the configuration keys and the default table names:

|key|default|
|---|-------|
|ad_analytics_by_creative_identifier|ad_analytics_by_creative|


If the connector uses different table names (for example ad_analytics_by_creative_test) this can be set in the `dbt_project.yml` as follows.

```yaml
vars:
    ad_analytics_by_creative_test_identifier: ad_analytics_by_creative_test
```

### (Optional) Step 4: Additional configurations

#### Change output tables:
The following vars can be used to change the output table names:

|key| default                  |
|---|--------------------------|
|linkedin_ad_performance_basic_v1_alias| linkedin-ad_performance_basic-v1 |


#### Add custom fields:
Ensure that the variable `linkedin_custom_fields` is defined in the root project's `dbt_project.yml` file (this is your main repository).
```yaml
# dbt_project.yml (root project)
vars:
  linkedin_custom_fields: "field1,field2,field3,field4"

```

#### Add non-existing fields:
Ensure that the variable `linkedin_non_existing_fields` is defined in the root project's `dbt_project.yml` file (this is your main repository).
This will add `NULL` value to the field.
```yaml
# dbt_project.yml (root project)
vars:
  linkedin_non_existing_fields: "field1,field2"

```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

# dbt-netezza

The `dbt-netezza` package contains all of the code required to make `dbt` operate on a Netezza database. For more information on using dbt, consult [their docs](https://docs.getdbt.com/docs).

## Netezza Configurations

### Performance Optimizations

Tables in Netezza have an optimization to improve query performance called distribution keys. Supplying these values as model-level configurations apply the corresponding settings in the generated `CREATE TABLE` DDL. Note that these settings will have no effect for models set to view or ephemeral models.

- `dist` can take a setting of `random`, a single column as a string (e.g. `visit_key`), or a list of columns (e.g. `['visit_key','visit_event_key']`)

Dist keys can be added to the `{{ config(...) }}` block for a specific model `.sql` file, e.g.:

```sql
-- Example with one sort key
{{ config(materialized='table', dist='visit_key') }}

select ...


-- Example with multiple sort keys
{{ config(materialized='table', dist=['visit_key', 'visit_event_key']) }}

select ...
```

Dist keys can also be added to the `dbt_project.yml` file config to set a default, e.g. 

```yaml
# dbt_project.yml
name: "my_project"
version: "0.0.1"
config-version: 2

...

models:
  my_project:
    +materialized: table
    +dist: random
```

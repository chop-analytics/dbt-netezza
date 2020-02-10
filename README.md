# dbt-netezza

The `dbt-netezza` package contains all of the code required to make `dbt` operate on a Netezza database. For more information on using dbt, consult [their docs](https://docs.getdbt.com/docs).

## Installation

1. Install the Netezza ODBC driver

2. Install `dbt-netezza`

```sh
# Install using pipenv (Recommended)

pipenv install git+https://github.research.chop.edu/analytics/dbt-netezza#egg=dbt-netezza
```

## Usage

1. [Create a `dbt` profile](https://docs.getdbt.com/docs/configure-your-profile)

```yaml
cdw:
  target: uat
  outputs:
    uat:
      type: netezza
      host: uat.cdw.chop.edu
      database: QMR_DEV
      schema: ADMIN
      username: <USERNAME>
      password: <PASSWORD>
```
2. [Create and use a `dbt` project](https://docs.getdbt.com/docs/creating-a-project)

# Ionis dbt Models
The home for all of Ionis's [dbt](https://www.getdbt.com/) data models.

## Contributing
In order to contribute, you will need to configure your local machine as described in the [Dependencies for development](#Dependencies-for-development) section. Once you have the dependencies set up, you can begin developing in your own sandbox environment.
### Dependencies for development
#### `VScode`
We use [VScode](https://code.visualstudio.com/) as our development IDE. This ensures everyone on our team has a consistent development experience and makes debugging setup issues easier (just copy what works for your teammate!). It is _STRONGLY_ recommended you install the following VScode extensions to improve your development workflow:
- [Advanced new file](https://marketplace.visualstudio.com/items?itemName=patbenatar.advanced-new-file)
- [Better Jinja](https://marketplace.visualstudio.com/items?itemName=samuelcolvin.jinjahtml)
- [Bracket Pair Colorizer 2](https://marketplace.visualstudio.com/items?itemName=CoenraadS.bracket-pair-colorizer-2)
- [dbt Power User](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user)
- [Python extension for VScode](https://marketplace.visualstudio.com/items?itemName=ms-python.python)
- [Pylance](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)
- [sqlfluff VScode](https://marketplace.visualstudio.com/items?itemName=dorzey.vscode-sqlfluff)
- [Visual Studio IntelliCode](https://marketplace.visualstudio.com/items?itemName=VisualStudioExptTeam.vscodeintellicode)
- [vscode-dbt](https://marketplace.visualstudio.com/items?itemName=bastienboutonnet.vscode-dbt)
- [Live Share Extension Pack](https://marketplace.visualstudio.com/items?itemName=MS-vsliveshare.vsliveshare-pack)
- [GitLens](https://marketplace.visualstudio.com/items?itemName=eamodio.gitlens)


**Please be sure to install [Python](https://www.python.org/downloads/) on your machine prior to moving foward with the steps below.**

### Configuring dbt `profile.yml`
You will need to add a `~/.dbt/profile.yml` file (home directory) in order for dbt to run models.

It is recommended that each developer use the naming convention of `dbt_{firstInitialLastName}` for their respective `dev` targets (schemas). For example, John Doe would have schema name of `dbt_jdoe`. Currently, the `sysadmin` will need to create each person's schema before use.

Please read the [documentation](https://docs.getdbt.com/dbt-cli/configure-your-profile) to further understand how the `profile.yml` configuration works within the dbt system.

Snowflake specific `profile.yml` documentation can be found [here](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile). It also provides a template for use.

Note: When setting up a new developer sandbox schema, there are certain settings required to achieve this behavior. First, the developer's new branch must be created as a clone of an existing branch. Second, the developer must create and save a custom 'profiles.yml' file to his or her local C Drive within the '.dbt' folder. That file must have the below format:

snowflake:

  target: sbx

  outputs:

    sbx:
      type: snowflake
      account: ionisph.west-us-2.azure



      # User/password auth
      user: <username>
      password: <password>

      role: DEVELOPER_DEV_ALL
      database: IONISDW_DEV
      warehouse: WH_DEVELOPMENT_DEV
      schema: dbt_<username>
      threads: 30
      client_session_keep_alive: False
      query_tag:

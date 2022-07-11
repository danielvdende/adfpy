# Deploying
Once you've written the code for your adfPy components, it's time to deploy them to ADF! 

The easiest way to do this is to use the deploy executable that is included in the adfPy package that you installed. To run it:
```shell
adfpy-deploy --path PATH
```
where `PATH` is the location of your adfPy resources. This includes both pipelines and triggers (and other ADF resources in the future).

## Configuration
In order to use `adfpy-deploy`, the following environment variables need to be set:

| Environment Variable                | Description                                                                                                        | Example |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------------| ------- |
| `AZURE_SUBSCRIPTION_ID`             | The id of the subscription in which your ADF instance is.                                                          |         |
| `AZURE_RESOURCE_GROUP_NAME`         | The name of the resource group in which your ADF instance is                                                       |         |
| `AZURE_DATA_FACTORY_NAME`           | The name of your ADF instance                                                                                      |         |
| `AZURE_SERVICE_PRINCIPAL_CLIENT_ID` | The client id of the Service Prinicpal you are using. This SP will need sufficient permissions to do XXX with ADF. |         |
| `AZURE_SERVICE_PRINCIPAL_SECRET`    | The corresponding secret for your Service Principal.                                                               |         |
| `AZURE_TENANT_ID`                   | The tenant id in which your ADF instance is deployed                                                               |         |

## Disable resource removal
By default, `adfpy-deploy` will try to synchronize whatever ADF resources you have in your defined path with whatever is present in the configured ADF instance. To disable this behaviour, please add the `--no-delete` parameter, e.g.
```shell
adfpy-deploy --path foo --no-delete
```
Setting `--no-delete` will prevent adfPy from removing any existing resources that are not on the configured path, but it will still add or update the resources you have in your path.
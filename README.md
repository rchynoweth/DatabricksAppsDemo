




1. **Following this walkthrough to start**: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/get-started 

    ```
    conda create -n DatabricksApps python=3.11 -y
    conda activate DatabricksApps
    pip install databricks-cli
    pip install gradio
    pip install pandas
    mkdir gradio-hello-world
    cd gradio-hello-world

    ```

1. Cost of a Databricks Application
    - 0.5x multiplier against 'PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_WEST_2'
        - https://learn.microsoft.com/en-us/azure/databricks/resources/pricing

    - Current value in the price column is: `0.950000000000000000`. 
    - So does this mean that a "Up to 2 vCPUs, 6 GB memory, 0.5 DBU/hour" hello world application is $0.02375 an hour? (0.5*0.5)*0.95=0.02375.  
        - Doesn't seem right. 
        - I will check back with my actual billing data. 
    ```
    select * 

    from system.billing.list_prices

    where sku_name like 'PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_WEST_2'

    order by price_start_time desc
    ```

1. The hello world app gets deployed to this URL - https://hello-world-1103871575580720.0.azure.databricksapps.com/. 

1. I assume that IP Access lists would act as a potential authenticator for the workspace? Do we have to have customer Entra ID authentication? 

1. Appears there is default authentication with Entra ID. I tested this through an Incognito browser. 


1. https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/auth

1. https://community.databricks.com/t5/administration-architecture/how-to-access-unitycatalog-s-volume-inside-databricks-app/td-p/121716

1. https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-apps/uc-volumes
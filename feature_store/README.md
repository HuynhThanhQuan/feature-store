# Feature Store

## Env

### DEV

### DataWarehouse

|Schema|DW_ANALYTICS|
|-----|-------|
|Hostname|192.168.124.100|
|Port|1521|
|Password| (request permission)|
|Service name| DWDEV|

|Schema|CINS_ADS|
|-----|-------|
|Hostname|192.168.124.100|
|Port|1521|
|Password| (request permission)|
|Service name| DWDEV|

| Schema | CINS_SMY |
|-----|-------|
|Hostname|192.168.124.100|
|Port|1521|
|Password| (request permission)|
|Service name| DWDEV|

### UAT

#### Jupyter Notebook

| Key | Value |
|-----|-------|
|Protocol| https |
|URL| cins01.apps.uat-cloud.sacombank.local|
|Location|/opt/bitnami/jupyterhub-singleuser/hcl-quanht/feature_store|
|Username|user2|
|Password| (request permission)|

## Folder Structure

```lua
feature_store
|-- config
|-- sql
|   |-- config
|   |-- template
|   |   |-- ddl
|   |   |-- dml
|   |   |-- feature
|-- store_procedure
|   |-- config
|   |-- template
|   |-- script
|-- check_DB.py
|-- gen_feature.py
|-- gen_script.py
|-- gen_table.py
|-- main.py
|-- oraDB.py
|-- ft_dependency.py
```

Tất cả code SQL feature được chứa tại sql/template/feature. Lưu ý đây là template cần truyền RPT_DT vào

## Abstract Concept

Dưới đây là Concept Flow để sinh ra Feature Store

![Flow](asset/FeatureStore-FeatureStoreFlow.jpg)




## Data Lineage

![DataLineage](asset/FeatureStore-DataLineage.jpg)

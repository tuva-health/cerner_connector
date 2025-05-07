This is a template to create connectors for The Tuva Project on version `0.14.x`. 

This template includes a general project structure for connectors for The Tuva Project, including the data models that The Tuva Project Package expects as inputs, some data tests and column-level documentation, and some notes on how to build on this connector in structuring your own projects.

### Deactivated Rows with the Cerner data model
There are times when a row is "deactivated" or soft-deleted. Depending on your use-case you may want to exclude these from your output.

Generally there are three ways Cerner deactivates rows:
1. active_ind
2. beg_effective_dt_tm and end_effective_dt_tm
3. active_status_cd

ACTIVE_IND is a basic flag, with 1 = active, 0 = inactive
BEG_EFFECTIVE_DT_TM and END_EFFECTIVE_DT_TM are like type 2 timestamp markers except the "active" end will be a "end of time" value like 12/31/2100 so you can reliably use WHERE END_EFFECTIVE_DT_TM > TODAY().
ACTIVE_STATUS_CD is a numeric value that ties to the CODE_VALUE table. The code in the transactional table can be variable, so you'll want to join to CODE_VALUE and use WHERE CODE_VALUE.CDF_MEANING = 'ACTIVE'.

As mentioned, not all modules use these consistently. Some tables may only need ACTIVE_IND. Some may use ACTIVE_IND as well as END_EFFECTIVE_DT_TM. Some may only use ACTIVE_STATUS_CD. 
So, you'll need to check the source table to determine how these are being used to determine which one or combination is appropriate.

### What is a connector?
Running a correctly-built connector prepares data to run through The Tuva Project dbt package. In effect, connectors help map raw data sources to the Tuva Data Model.

### Connector Project Structure
As a general pattern, connectors are roughly 1:1 with raw data sources, because each raw data source often has its own unique challenges. The typical workflow and project structure for mapping raw data to the Tuva Data Model within a connector is:
* `staging` layer: `source()` raw data and map it to the Tuva Data Model
* `int` layer: handle any consequential transformations, including Adjustments, Denials, and Reversals (ADR) for claims and deduplication.
* `final` layer: data is ready to run through The Tuva Project—the models in this layer are expected by The Tuva Project Package.

### Running a connector
When you've completed mapping, and you're ready to run the connector without running the whole Tuva Project, you can build and test all models by running:
```console
dbt build --full-refresh -s tag:input_layer
```

### Tuva Resources:
- The Tuva Project [docs](https://thetuvaproject.com/)
- The Tuva Project [dbt docs](https://tuva-health.github.io/tuva/#!/overview/)
- The Tuva Project [package](https://hub.getdbt.com/tuva-health/the_tuva_project/latest/)

### dbt Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

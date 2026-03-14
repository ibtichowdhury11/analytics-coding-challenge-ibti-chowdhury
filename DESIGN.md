# Design Document

I have not included approaches for dim_publishers and stg_ad_units due to time and because they are basic selects or utilising the same methods. 

## 1) Your data modeling approach

Stg_ad_events

- I structured the staging layer to act as the cleaner of the data and deduplicate the events using the window function to get the latest event id as that was not unique until the window function. 
- implemented a pre hook that deletes the specific date range from the destination table before the new data is processed. While a unique_key handles row level updates, the Delete + Insert strategy is more robust for this dataset. It ensures that if source data is updated or deduplicated, the old records are entirely wiped and replaced with the fresh source of truth. (running the model multiple times will never result in duplicate data.) 
- The pre hook is an equivalent of GCP pre operations but I had to also use mutations_sync to make it behave the same way as my first attempt ran the new data before the removal of the old. To replicate the behavior of GCP pre operations, I implemented the DELETE pre hook using mutations_sync = 2. This was a deliberate choice to force ClickHouse to process deletions synchronously before the INSERT phase began.
- I have also applied a full refresh option from the minimum date found so the table can full refresh if any issues occur in the future such as data issues. 
- unique & not_null (on event_id): Since I identified that event_id appears more than once in the source, these tests prove that my deduplication logic is working. They ensure no event is counted twice and no data is anonymous.
- not_null (on event_timestamp): Because the pipeline relies on a 10 day lookback window, a NULL timestamp would bypass both the incremental filter and the pre hook deletion logic. This test ensures every row is accounted for within our temporal boundaries.
- Is_filled accepted arguments is either a 0 or 1, it will alert me if invalid data (like a 3 or a NULL) enters the system.
- I chose to partition the data by event_date, This is what makes the 10 day lookback window so efficient. ClickHouse can quickly drop or modify specific partitions rather than scanning the entire table. It also speeds up queries significantly. 
- I set the data to be stored in order of Date > Publisher > Ad Unit. This makes the dataset run much faster. 
- This design allows the business to get answers quickly without scanning billions of raw rows. We can instantly calculate key metrics like total revenue and fill rates. Because I kept the Staging layer at the event level, we still have the ability to drill down into specific details if a daily number looks suspicious. (I would look to drop event_id in the final select if the size of the table is very big and taxing.)

Stg_campaigns 

- I structured the staging layer to be a clean, unique version of the campaign metadata. Because there are only 27 rows in the table, I materialised it as a standard table, which is the most efficient way to handle such a small dataset. I used a window function to deduplicate the campaign IDs, ensuring that only the most recent information is passed through to the marts for joining with our event data.
- This design allows the business to accurately track spend and performance across different dimensions. By standardising device types and advertiser IDs, the data is ready for immediate grouping and filtering. The table is ordered by Advertiser ID > Campaign ID, this ensures that any business query looking at a specific advertiser’s performance is processed almost instantly. 

Stg_publishers 

- I structured the staging layer to be the source of truth for all publisher metadata. Because there are only 21 rows, materialising it as a table is the most straightforward and reliable approach. I used a window function to pick the latest version of each publisher based on their update time, ensuring the table is always current.
- This design supports the business by providing a clean, searchable list of partners. By standardising categories and casing, I’ve made it easy for analysts to create reports by country or category. The use of the updated_at logic means that if a publisher changes their account manager or domain, the business will always see the most recent information in their dashboards.
- I applied "unique" and "not_null" tests to the publisher ID. This proves that my deduplication logic worked correctly and ensures that every publisher has a valid, identifiable ID for joining with our events and campaigns tables.
- I also added a "not_null" test to the publisher name. This ensures that we do not have any "nameless" publishers in our reporting, which would make the data difficult for account managers and business users to interpret.
- I applied a filter to include all publishers updated from 2023 onwards. This date was chosen because it represents the minimum "created_at" date in our source data. By including everything from this point, I have ensured that we capture all historical publishers in the system. This was a deliberate choice to support the business in case users need to look back at historical reporting or compare performance with older partners that are no longer active.

Fct_ad_events_daily 

- I implemented an incremental strategy using a pre hook that deletes the last 10 days of data before inserting the new calculations. I used the "mutations_sync = 2" setting to force the database to finish the deletion before starting the insert. This acts exactly like a pre-operation in GCP, ensuring that if we run the model multiple times, we never end up with duplicate rows or a mix of old and new data. (this is the same logic applied in the dependency table) 
- I structured this marts model as an incremental fact table that aggregates raw events into daily summaries. It pulls clean data from the staging layer and joins it with the publisher dimension table to add descriptive names and categories. By using a 10 day lookback window, the model stays up to date with any late arriving data or corrections from the source system while keeping the daily run times very short.
- The grain of this table is one row per Date, Publisher, Ad Unit, Site, Campaign, Advertiser, Device, Browser, and Country. This is a granular fact grain, meaning we have kept all the important details while still pre calculating the totals for revenue, impressions, and clicks. 
- I built the semantic layer directly into the dbt YAML file to act as a bridge between the raw data and the end user. By defining metrics like total revenue and impressions within the code, I ensure that every dashboard or reporting tool uses the exact same calculation. This prevents different teams from getting different numbers for the same metric, creating a single source of truth for the entire business.
- I chose to define the Fill Rate as a calculated metric in the semantic layer rather than a static column in the table. In BI tools, you cannot simply sum up a percentage column from a table, as that would give you a mathematically incorrect result. By defining the division logic (Total Filled / Total Requests) in the semantic layer, the metric becomes dynamic. This means that whether a user looks at the data by Day, by Publisher, or across the entire Year, the BI tool will first sum the raw totals and then perform the division, ensuring the Fill Rate is always accurate at any level of aggregation. Or I would create this metric in the BI tool modelling section as a measure. Looker being LookML or power BI being dax as an example. 
- I partitioned the table by date and ordered it by Date, Site Domain, and Device Type. This setup allows ClickHouse to skip over years of data and only scan the specific days required for a report. Ordering by domain and device type further optimises the table by grouping similar traffic together, which significantly speeds up the most common business queries.

## 2) Data quality issues found and handling


Stg_ad_events

- I discovered events with timestamps dated for the future, such as 2026-03-18, which would make our daily reporting look impossible and incorrect. To fix this, I added a filter that caps the data at now(), ensuring we only show events that have actually occurred in the real world. While this might filter out rows with broken system clocks, it is a necessary trade off to keep our business reports trustworthy.
- The raw data contained multiple entries for the same event_id, which would have caused us to double-count revenue and total events. I solved this by using a ranking function to pick the latest version of an event based on when it was loaded into the system.
- I noticed that fields like country codes were a mix of upper and lower case, which prevented the system from grouping "US" and "us" together correctly. I handled this by forcing all these text fields to lowercase so that they are standardised for every user.
- From what I remember, ad bidding is done in tiny micro amounts, using standard numbers could lead to rounding errors that make our revenue look like zero. I handled this by casting these values to a specific decimal format that keeps six places of precision. 
- My chosen approach chooses accuracy and consistency over raw processing speed. By using ranking functions to remove duplicates and high precision decimals for revenue, the pipeline takes a bit more effort to run, but the final numbers are guaranteed to be correct. I also chose to standardise text to lowercase and turn zeros into nulls, while this changes the original look of the raw data, it makes the data much easier for everyone to use and prevents confusion during analysis. 

Stg_campaigns

- While I noticed the campaign status column currently only shows a few variations, I decided not to apply an accepted values test yet. It is highly likely that new statuses like "deleted" or "paused" exist but are simply not present in the current 27 rows. By leaving this test out for now, I avoid the risk of the pipeline failing as soon as a valid new status appears. 
- I identified that campaign IDs were not unique in the raw source, which would have caused errors when joining with other tables. I handled this by using a ranking function to pick the latest version of each campaign based on the creation time. While this approach only keeps the most recent record, it ensures the dimension table remains a clean and unique list for the business to use.
- I cast the campaign budgets to a decimal format with two places of precision and converted the timestamps into simple dates. This keeps financial figures accurate and removes unnecessary time details that are not needed for campaign level reporting.

Stg_publishers

- I noticed that the publisher categories were inconsistent, specifically with the use of "mobile_gaming" containing an underscore. I transformed this to "mobile gaming" to ensure the format matches the rest of the items in the column
- The raw data contained multiple entries for the same publisher ID. I decided to deduplicate these by using the "updated_at" column, with the assumption that the most recent timestamp reflects the latest and most accurate metadata for that publisher.
- I found that website domains and country codes were not uniform in their casing. I standardised the primary domain to lowercase and forced the country codes to uppercase. This makes joining and filtering much more reliable, as it prevents the logic from treating "UK" and "uk" as two different countries.

Fct_ad_events_daily

- I created the unique_key by generating an MD5 hash of the entire row grain, including the date, IDs, and all descriptive dimensions. This matters because, in an incremental model, we need a reliable way to identify each specific row of data. By hashing these fields together, I ensured that every unique combination of date, publisher, campaign, and device has a persistent ID.
- An MD5 hash takes all that information and squashes it into a standard 32-character string. This makes the unique key predictable in size, which helps the database store and search for records much more efficiently. 
- I used the COALESCE function in the unique_key to replace any null values with the string "unknown" for dimensions and "0" for IDs. If any part of the unique key calculation contains a null, the entire hash could fail or become null itself, which would break the table's primary key and the incremental loading logic. In BI tools, nulls often behave unpredictably for example, a filter for Device Type might completely ignore rows where the device is null. By forcing these to "unknown", I ensure that every single event is visible and can be filtered or grouped by the business users.

## 4) Production readiness

- For orchestration, I made an attempt to showcase in the branch by creating a schedule.yml file with a cron schedule. Staging tables run at 8am and mart tables run at 9am everyday. The 9am is an assumption that people in the business look at reporting as a part of their daily workflow. I applied tag names in the config of my tables. I have also created a full refresh tag which is a manual tag to be used if the situation requires it such if a major data quality issue is discovered or if business logic changes.
- When creating pipelines I will document the work in confluence with information on certain characteristics or gotcha moments so others in the team can be educated on it or want to help with pipeline development. 
- I have created variables to be used so I can change date variables in one place rather than doing the same changes in multiple scripts. 
- I would look to implement assertion files (singular tests) in more detail rather than just tests within yaml files. 
- I would look to implement singular test notifications via a tool like slack to notify of any failures. 
- I would add an owner tag or a contact field directly into your dbt schema.yml or in the config block if that is allowed like GCP. This means anyone looking knows exactly who to contact. The owner would be the technical lead.  

## 5) Lightdash notes
I had some technical difficulties connecting the models to lightdash, due to this I made the decision to make a dashboard on looker studio via exporting the fct_ad_event_daily table into a csv and using that as the data source. I rather show insights and dashboard than not providing anything at all. https://lookerstudio.google.com/s/ltIKL3Cd4n0 

Date range 10th feb - 11th march

- Summarize key insights from your charts.

Total Revenue Over Time
- Revenue is consistent, averaging approximately $1,130 per day. The stability suggests a steady baseline of traffic and demand, with no major seasonal dips or technical outages during the time period.

Fill Rate Analysis 
- The majority of publishers fall into the 84% to 86% range. This indicates that the supply and demand are well matched. 
- GameRant has a fill rate of 94.17%, the ad units or content strategy they are using is capturing almost every single request. (I would look to us ad unit look up table to see what type of ad unit is bringing this success) 
- Screen Rant Gaming has a fill rate of 84.22%, while that is not bad, it is the lowest in the list during the period. If looking to optimise, I would look to investigate any timeouts or ad blocking issues. 
- I noticed a spike in clicks on the 24th of February while all other days for clicks have been stable. The click spike on February 24th was almost entirely driven by a single publisher browser country combination. GameRant saw 512 clicks specifically from Samsung Internet users in Brazil. From looking online it could be due to The Samsung Galaxy S26 series was officially unveiled at Galaxy Unpacked on February 25th, however, this theory is weak due to only spiking in one country. 

Potential red flags 
- I noticed that the following publishers have recorded 0 clicks: Nintendo Life, Screen Rant Gaming, and VG247. For VG247, having 2,000 impressions and 0 clicks is suspicious. This might indicate tracking issues where impressions are being counted, but the click redirect is broken.

Reflection on test 

- Coming from a different tech stack, I’ve focused on translating the analytics engineering principles I know into the Venatus environment. I’m already very comfortable with the SQL and dbt config blocks, but it’s been a real eye-opener to see how things are tied together here using Docker and ClickHouse.
- I haven't dived deep into backfill strategies in this specific setup yet, as I assume the patterns used at Venatus will be more specialised for your scale. I’d definitely need a bit of a walkthrough on the specific orchestration tools you use, but after seeing how the team operates, I’m confident I’ll pick it up quickly. I’m also keen to get under the hood of the overall architecture, specifically how the containers and file structures are configured, as I know that’s key to getting the best performance out of ClickHouse.
- I have an understanding of pipeline dependencies and data lifecycles, so once I’m exposed to the internal tooling and infrastructure, I’ll be able to master it fast until it just becomes a natural habit in my daily workflow. My goal is to move quickly from learning the stack to contributing best practices to the team.
- If i did this test again I would look to edit my MR requests to have a chance to make things less heavy on the eyes. In my current work place it is easy to edit the MR requests but I found some issues trying to do the same. 

Issue logs 
- Macbook did not let docker download the intel chip version even though my macbook is the latest version of OS. tried to fix which did take 20 mins of time but then switched to codepace for browser vs code. 
- I did notice some orphan container errors, though it's a light warning, I am not too familiar with containers but I assume if I drop it things may start to go wrong and be harder to diagnose. 
- The codespace and its connected ports sometimes time out very quickly which forces me to rebuild the codespace. This is where some time did cost me but i did learn as I went on how to get around this. 
- While the primary goal was to utilise Lightdash, I encountered these connectivity/configuration issues within the specific Docker environment. Rather than getting stuck on a UI hurdle, I made the call to pivot to Looker Studio to ensure I met the ask and could actually showcase the data. I exported the curated fct_ad_events_daily mart to build the final dashboard.
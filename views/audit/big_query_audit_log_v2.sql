/*
 * Script: BQ Audit Version 2
 * Author: NamrataShah5
 * Description:
 * This SQL Script creates a materialized source table based on the newer BigQueryAuditMetadata
 * stackdriver logs. This script acts AS input to the Dashboard.
 * Reference for BigQueryAuditMetadata: https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata
 */
 WITH query_audit AS (
    SELECT
      protopayload_auditlog.authenticationInfo.principalEmail,
      resource.labels.project_id AS projectId,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.requestMetadata.callerIp') AS callerIp,
      protopayload_auditlog.serviceName,
      protopayload_auditlog.methodName,
      COALESCE(
        CONCAT(
          SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'),"/")[SAFE_OFFSET(1)],
          ":",
          SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'),"/")[SAFE_OFFSET(3)]
        ),
        CONCAT(
          SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName'),"/")[SAFE_OFFSET(1)],
          ":",
          SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName'),"/")[SAFE_OFFSET(3)]
        )
      ) AS jobId,
      /*
       * All queries related to jobStats
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstats
       */
      JSON_EXTRACT_SCALAR(
       protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.parentJobName') AS parentJobName,
      COALESCE(
        TIMESTAMP(JSON_EXTRACT_SCALAR(
         protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStats.createTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(
         protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.createTime'))
      ) AS createTime,
      COALESCE(
        TIMESTAMP(JSON_EXTRACT_SCALAR(
         protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStats.startTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(
         protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.startTime'))
      ) AS startTime,
      COALESCE(
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.endTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.endTime'))
      ) AS endTime,
      COALESCE(
       TIMESTAMP_DIFF(
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
           '$.jobInsertion.job.jobStats.endTime')),
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
           '$.jobInsertion.job.jobStats.startTime')),
          MILLISECOND),
        TIMESTAMP_DIFF(
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
           '$.jobChange.job.jobStats.endTime')),
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
           '$.jobChange.job.jobStats.startTime')),
          MILLISECOND)
      ) AS runtimeMs,
      COALESCE(
        TIMESTAMP_DIFF(
          TIMESTAMP(JSON_EXTRACT_SCALAR(
           protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStats.endTime')
          ),
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.startTime')),
        SECOND),
        TIMESTAMP_DIFF(
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.endTime')),
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.startTime')),
          SECOND
        )
      ) AS runtimeSecs,
      COALESCE(
        CAST(CEILING(
              TIMESTAMP_DIFF(
                TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                  '$.jobInsertion.job.jobStats.endTime')),
                TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                  '$.jobInsertion.job.jobStats.startTime')),
                SECOND) / 60 ) AS INT64),
        CAST(CEILING(
              TIMESTAMP_DIFF(
                TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                  '$.jobChange.job.jobStats.endTime')),
                TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                  '$.jobChange.job.jobStats.startTime')),
                SECOND) / 60) AS INT64) 
      ) AS executionMinuteBuckets,
        COALESCE(
          CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.totalSlotMs') AS INT64),
          CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.totalSlotMs') AS INT64)
        ) AS totalSlotMs,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.reservationUsage.name'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.reservationUsage.name')
      ) AS reservationUsageName,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.reservationUsage.slotMs'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.reservationUsage.slotMs')
      ) AS reservationUsageSlotMs,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.totalProcessedBytes'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.totalProcessedBytes')
      ) AS totalProcessedBytes,
      COALESCE(
        CAST(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.totalBilledBytes') AS INT64),
         CAST(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.totalBilledBytes') AS INT64)
      ) AS totalBilledBytes,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.billingTier'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.billingTier')
      ) AS billingTier,
      SPLIT(TRIM(TRIM(
        COALESCE(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.referencedTables'),
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedTables')),
          '["'),'"]'),'","') AS referencedTables,
      ARRAY_LENGTH(SPLIT(
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.referencedTables'), 
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedTables')
        ), ",")
      ) AS totalTablesProcessed,
      SPLIT(TRIM(TRIM(
        COALESCE(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.referencedViews'),
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedViews')),
          '["'),'"]'),'","') AS referencedViews,
      ARRAY_LENGTH(SPLIT(
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.referencedViews'), 
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedViews')
        ), ",")
      ) AS totalViewsProcessed,
      SPLIT(TRIM(TRIM(
        COALESCE(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.referencedRoutines'),
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedRoutines')),
          '["'),'"]'),'","') AS referencedRoutines,
      ARRAY_LENGTH(SPLIT(
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.referencedRoutines'), 
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedRoutines')
        ), ",")
      ) AS totalRoutinesProcessed,
      COALESCE(
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.referencedRoutines'),"/")[SAFE_OFFSET(1)],
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedRoutines'),"/")[SAFE_OFFSET(1)]
      ) AS refRoutine_project_id,
      COALESCE(
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.referencedRoutines'),"/")[SAFE_OFFSET(3)],
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedRoutines'),"/")[SAFE_OFFSET(3)]
      ) AS refRoutine_dataset_id,
      COALESCE(
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.referencedRoutines'),"/")[SAFE_OFFSET(5)],
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedRoutines'),"/")[SAFE_OFFSET(5)]
      ) AS refRoutine_table_id,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.outputRowCount'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.outputRowCount')
      ) AS outputRowCount,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.cacheHit'), 
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.cacheHit')
      ) AS cacheHit,
      COALESCE(
        CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.loadStats.totalOutputBytes') AS INT64),
        CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.loadStats.totalOutputBytes') AS INT64)
      ) AS loadStatsTotalOutputBytes,
      /*
       * Queries related to JobStatus
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstatus
       */
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStatus.jobState'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStatus.jobState')
      ) AS jobState,
      SPLIT(COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStatus.errorResult'),JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStatus.errorResult.code')),"/")[SAFE_OFFSET(1)],
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStatus.errorResult.code'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStatus.errorResult.code')
      ) AS errorResultCode,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStatus.errorResult.message'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStatus.errorResult.message')
      ) AS errorResultMessage,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStatus.errorResult.details'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStatus.errorResult.details')
      ) AS errorResultDetails,
      REGEXP_EXTRACT_ALL(COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStatus.error'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStatus.error') 
        ),r'"message":\"(.*?)\"}'
      ) AS errorMessage,
      REGEXP_EXTRACT_ALL(COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStatus.error'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStatus.error') 
        ),r'"code":\"(.*?)\"}'
      ) AS errorCode,
      /*
       * Queries related to loadConfig job
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load
       */
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.loadConfig.sourceUrisTruncated'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.loadConfig.sourceUrisTruncated')) 
      AS loadsourceUrisTruncated,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.loadConfig.schemaJsonUrisTruncated'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.loadConfig.schemaJsonUrisTruncated')) 
      AS loadschemaJsonTruncated,
      SPLIT(TRIM(TRIM(
        COALESCE(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.queryStats.sourceUris'),
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.sourceUris')),
          '["'),'"]'),'","') AS loadSourceUris,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.loadConfig.createDisposition'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.loadConfig.createDisposition')) 
      AS loadcreateDisposition,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.loadConfig.writeDisposition'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.loadConfig.writeDisposition')) 
      AS loadwriteDisposition,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.loadConfig.schemaJson'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.loadConfig.schemaJson')) 
      AS loadschemaJson,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.loadConfig.destinationTable'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.loadConfig.destinationTable')) AS loadDestinationTable,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.loadConfig.destinationTableEncryption.kmsKeyName'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.loadConfig.destinationTableEncryption.kmsKeyName')
      ) AS loadkmsKeyName,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobConfig.loadConfig.load'),
      /*
       * Queries related to queryConfig job
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#query
       */
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.queryTruncated'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.queryTruncated')) 
      AS queryTruncated,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.schemaJsonUrisTruncated'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.schemaJsonUrisTruncated')) 
      AS queryschemaJsonTruncated,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.createDisposition'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.createDisposition')) 
      AS querycreateDisposition,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.priority'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.priority')) 
      AS queryPriority,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.defaultDataset'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.defaultDataset')) 
      AS querydefaultDataset,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.writeDisposition'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.writeDisposition')) 
      AS querywriteDisposition,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.schemaJson'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.schemaJson')) 
      AS queryschemaJson,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.destinationTable'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.destinationTable')) 
      AS queryDestinationTable,
      SPLIT(
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobConfig.queryConfig.destinationTable'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.destinationTable')),"/")[SAFE_OFFSET(1)] 
      AS querydestTable_project_id,  
      SPLIT(
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobConfig.queryConfig.destinationTable'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.destinationTable')),"/")[SAFE_OFFSET(3)] 
      AS querydestTable_dataset_id,   
      SPLIT(
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobConfig.queryConfig.destinationTable'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobConfig.queryConfig.destinationTable')),"/")[SAFE_OFFSET(5)] 
       AS querydestTable_table_id,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.destinationTableEncryption.kmsKeyName'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.destinationTableEncryption.kmsKeyName')
      ) AS querykmsKeyName,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.query'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.query')
      ) AS query,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.statementType'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.statementType')
      ) AS statementType,
      /*
       * Queries related to tableCopyConfig
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablecopy
       */
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable') AS tableCopydestinationTable, 
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.createDisposition') AS tableCopycreateDisposition,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.writeDisposition') AS tableCopywriteDisposition,
      SPLIT(TRIM(TRIM(
        COALESCE(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobConfig.tableCopyConfig.sourceTables'),
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobConfig.tableCopyConfig.sourceTables')),
      '["'),'"]') ,'","') AS tableCopysourceTables,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.tableCopyConfig.sourceTablesTruncated') AS tableCopysourceTablesTruncated,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTableEncryption.kmsKeyName'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.tableCopyConfig.destinationTableEncryption.kmsKeyName')
      ) AS tableCopykmsKeyName,
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable'),
        ".")[SAFE_OFFSET(1)] AS tableCopyproject_id,
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable'),
        ".")[SAFE_OFFSET(2)] AS tableCopydataset_id,
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable'),
        ".")[SAFE_OFFSET(3)] AS tableCopytable_id,
      /*
       * Queries related to extractConfig
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#extract
       */
      SPLIT(TRIM(TRIM(
        COALESCE(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobConfig.extractConfig.destinationUris'),
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobConfig.extractConfig.destinationUris')),
          '["'),'"]'),'","') AS extractdestinationUris,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.extractConfig.destinationUrisTruncated'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.extractConfig.destinationUrisTruncated'))
      AS extractdestinationUrisTruncated,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.extractConfig.sourceTable') AS extractsourceTable,
      SPLIT(
         JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.extractConfig.sourceTable')
      ,",")[SAFE_OFFSET(1)] AS extract_projectid,
      SPLIT(
         JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.extractConfig.sourceTable')
      ,",")[SAFE_OFFSET(3)] AS extract_datasetid,
      SPLIT(
         JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.extractConfig.sourceTable')
      ,",")[SAFE_OFFSET(5)] AS extract_tableid,
      /*
       * The following code extracts the columns specific to the Load operation in BQ
       */
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, "$.jobChange.after") AS jobChangeAfter,
      REGEXP_EXTRACT(protopayload_auditlog.metadataJson, 
        r'BigQueryAuditMetadata","(.*?)":') AS eventName,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.labels.querytype'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.labels.querytype')
      ) AS querytype
    FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
  ),
  /*
   * Data from tableDataChange audit logs
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledatachange
   */
  data_audit AS (
    SELECT
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.datasetCreation.dataset.insertedRowsCount') AS insertRowCount,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
        '$.tableDataChange.deletedRowsCount') AS deleteRowCount,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
        '$.tableDataChange.reason') AS tableDataChangeReason,
      CONCAT(
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDataChange.jobName'),
        "/")[SAFE_OFFSET(1)], 
        ":",
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.tableDataChange.jobName'),
          "/")[SAFE_OFFSET(3)]
      ) AS jobId
    FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
  ),
  /*
   * Data from TableCreation and TableChange audit logs
   * TableCreation: https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablechange
   * TableChange: https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablechange
   */
  table_audit AS (
    SELECT
      COALESCE(CONCAT(
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
              '$.tableCreation.jobName'),
              "/")[SAFE_OFFSET(1)], 
        ":",
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.tableCreation.jobName'),
              "/")[SAFE_OFFSET(3)]
        ),
       CONCAT(
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
              '$.tableChange.jobName'),
              "/")[SAFE_OFFSET(1)], 
        ":",
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.tableChange.jobName'),
              "/")[SAFE_OFFSET(3)]
      )) AS jobId,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.tableName'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.tableName')
      ) AS tableName,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.tableInfo.friendlyName'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.tableInfo.friendlyName')
      ) AS tableFriendlyName,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.tableInfo.description'), 
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.tableInfo.description')
      ) AS tableDescription,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.schemaJson'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.schemaJson')
      ) AS tableSchemaJson,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.schemaJsonTruncated'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.schemaJsonTruncated')
      ) AS tableSchemaJsonTruncated,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.view.query'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.view.query')
      ) AS tableQuery,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.view.queryTruncated'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.view.queryTruncated')
      ) AS tableTruncated,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.expireTime'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.expireTime')
      ) AS tableExpireTime,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.createTime'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.createTime')
      ) AS tableCreateTime,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.updateTime'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCrhange.table.updateTime')
      ) AS tableUpdateTime,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.truncateTime'), 
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.truncateTime')
      ) AS tableTruncateTime,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.encryption.kmsKeyName'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.encryption.kmsKeyName')
      ) AS tableKmsKeyName,
      COALESCE(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableCreation.table.reason'),
         JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.table.tableChange.reason') 
      ) AS tableReason,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
          '$.tableChange.table.truncated')
       AS tableChangeTruncated
    FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access` ),
  /*
   * Data from tableDeletion audit logs
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledeletion
   */
  tableDeletion_audit AS (
    SELECT
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
        '$.tableDeletion.table.reason') AS tableDeletionReason,
      COALESCE(
        CONCAT(
          SPLIT(
            JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableCreation.jobName'),
              "/")[SAFE_OFFSET(1)], 
          ":",
          SPLIT(
            JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableCreation.jobName'),
              "/")[SAFE_OFFSET(3)]
          ),
       CONCAT(
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
              '$.tableChange.jobName'),
              "/")[SAFE_OFFSET(1)], 
        ":",
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.tableChange.jobName'),
              "/")[SAFE_OFFSET(3)]
      )) AS jobId
    FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`),
   /*
    * Data from tableDataRead audit logs
    * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledataread
    */
   tableDataRead_audit AS (
    SELECT
      CONCAT(
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
              '$.tableDataRead.jobName'),
              "/")[SAFE_OFFSET(1)], 
        ":",
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.tableDataRead.jobName'),
              "/")[SAFE_OFFSET(3)]
      ) AS jobId,
      SPLIT(TRIM(TRIM(
        COALESCE(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataRead.fields'),
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataRead.fields')),
          '["'),'"]'),'","') AS fieldsAccessed,
       JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataRead.fieldsTruncated') AS fieldsTruncated,
       SPLIT(TRIM(TRIM(
          JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataRead.categories'),
          '["'),'"]'),'","') AS categories,
       JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataRead.categoriesTruncated') AS categoriesTruncated,
       JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataRead.reason') AS tableDataReadReason,
       JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataRead.sessionName') AS sessionName
    FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
   ),
   /*
    * Data from modelDeletion audit logs
    * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modeldeletion
    */
   modelDeletion_audit AS (
     SELECT 
       JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.modelDeletion.reason') AS modelDeletionReason,
       CONCAT(
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
              '$.modelDeletion.jobName'),
              "/")[SAFE_OFFSET(1)], 
        ":",
        SPLIT(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.modelDeletion.jobName'),
              "/")[SAFE_OFFSET(3)]
      ) AS jobId,
     FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
   ),
   /*
    * Data from ModelMetadataChange audit logs
    * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelmetadatachange
    */
   modelMetadataChange_audit AS (
     SELECT
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.reason') AS modelMetadataChangeReason,
       CONCAT(
         SPLIT(
           JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
             '$.modelMetadataChange.jobName'),
              "/")[SAFE_OFFSET(1)],
         ":",
         SPLIT(
           JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
           '$.modelMetadataChange.jobName'),
            "/")[SAFE_OFFSET(3)]
       ) AS jobId,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.modelName') AS modelMetadataChangeModelName,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.modelInfo.entityInfo.friendlyName') AS modelMetadataChangeEntityInfoFriendlyName,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.modelInfo.entityInfo.description') AS modelMetadataChangeEntityInfoDescription,
       JSON_EXTRACT(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.modelInfo.entityInfo.labels') AS modelMetadataChangeEntityInfoLabels,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.expireTime') AS modelMetadataChangeModelExpireTime,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.createTime') AS modelMetadataChangeModelCreateTime,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.updateTime') AS modelMetadataChangeModelUpdateTime,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelMetadataChange.model.encryption.kmsKeyName') AS modelMetadataChangeEncryptionKmsKeyName,
     FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
   ),
   /*
    * Data from ModelCreation audit logs
    * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelcreation
    */
   modelCreation_audit AS (
     SELECT
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.reason')  AS modelCreationReason,
       CONCAT(
         SPLIT(
           JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
             '$.modelCreation.jobName'),
              "/")[SAFE_OFFSET(1)],
         ":",
         SPLIT(
           JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
           '$.modelCreation.jobName'),
            "/")[SAFE_OFFSET(3)]
       ) AS jobId,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.modelName')  AS modelCreationModelName,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.modelInfo.entityInfo.friendlyName')  AS modelCreationEntityInfoFriendlyName,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.modelInfo.entityInfo.description')  AS modelCreationEntityInfoDescription,
       JSON_EXTRACT(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.modelInfo.entityInfo.labels')  AS modelCreationEntityInfoLabels,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.expireTime')  AS modelCreationModelExpireTime,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.createTime')  AS modelCreationModelCreateTime,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.updateTime')  AS modelCreationModelUpdateTime,
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelCreation.model.encryption.kmsKeyName')  AS modelCreationEncryptionKmsKeyName,
     FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
   ),
  /*
   * Data from modelDataChange audit logs:
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modeldatachange
   */
  modelDataChange_audit AS (
      SELECT
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
              '$.modelDataChange.reason')
          AS modelDataChangeReason,
          CONCAT(
            SPLIT(
              JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                  '$.modelDataChange.jobName'),
                  "/")[SAFE_OFFSET(1)], 
            ":",
            SPLIT(
              JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                '$.modelDataChange.jobName'),
                  "/")[SAFE_OFFSET(3)]
          ) AS jobId,
      FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
   ),
   routine_audit AS (
      SELECT
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
              '$.routine.reason')
          AS routineReason,
          COALESCE(
            CONCAT(
              SPLIT(
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                  '$.routineCreation.jobName'),
                  "/")[SAFE_OFFSET(1)], 
              ":",
              SPLIT(
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                  '$.routineCreation.jobName'),
                    "/")[SAFE_OFFSET(3)]
            ),
            CONCAT(
              SPLIT(
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                  '$.routineChange.jobName'),
                  "/")[SAFE_OFFSET(1)], 
              ":",
              SPLIT(
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                  '$.routineChange.jobName'),
                    "/")[SAFE_OFFSET(3)]
            ),
            CONCAT(
              SPLIT(
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                  '$.routineDeletion.jobName'),
                  "/")[SAFE_OFFSET(1)], 
              ":",
              SPLIT(
                JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
                  '$.routineDeletion.jobName'),
                    "/")[SAFE_OFFSET(3)]
            )
          ) AS jobId,
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineCreation.routine.routineName'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineChange.routine.routineName'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineDeletion.routine.routineName')
        ) AS routineName,
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineCreation.routine.createTime'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineChange.routine.createTime'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineDeletion.routine.createTime')
        ) AS routineCreateTime,
        COALESCE(
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineCreation.routine.updateTime'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineChange.routine.updateTime'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, 
            '$.routineDeletion.routine.updateTime')
        ) AS routineUpdateTime
      FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
   )
SELECT
  principalEmail,
  callerIp,
  serviceName,
  methodName,
  eventName,
  tableDataChangeReason,
  statementType,
  jobState,
  errorResultCode,
  errorResultMessage,
  errorResultDetails,
  errorCode,
  errorMessage,
  projectId,
  jobId,
  querytype,
  insertRowCount,
  deleteRowCount,
  outputRowCount,
  tableName,
  tableFriendlyName,
  tableDescription,
  tableSchemaJson,
  tableSchemaJsonTruncated,
  tableQuery,
  tableTruncated,
  tableExpireTime,
  tableCreateTime,
  tableUpdateTime,
  tableTruncateTime,
  tableKmsKeyName,
  tableReason,
  tableChangeTruncated,
  tableDeletionReason,
  fieldsAccessed,
  fieldsTruncated,
  categories,
  categoriesTruncated,
  tableDataReadReason,
  sessionName,
  routineReason,
  routineName,
  routineCreateTime,
  routineUpdateTime,
  STRUCT(
    EXTRACT(MINUTE FROM startTime) AS minuteOfDay,
    EXTRACT(HOUR FROM startTime) AS hourOfDay,
    EXTRACT(DAYOFWEEK FROM startTime) - 1 AS dayOfWeek,
    EXTRACT(DAYOFYEAR FROM startTime) AS dayOfYear,
    EXTRACT(WEEK FROM startTime) AS week,
    EXTRACT(MONTH FROM startTime) AS month,
    EXTRACT(QUARTER FROM startTime) AS quarter,
    EXTRACT(YEAR FROM startTime) AS year
  ) AS date,
  createTime,
  startTime,
  endTime,
  runtimeMs,
  runtimeSecs,
  cacheHit,
  REGEXP_CONTAINS(jobId, 'beam') AS isBeamJob,
  REGEXP_CONTAINS(query, 'cloudaudit_googleapis_com_data_access') AS isAuditDashboardQuery,
  errorCode IS NOT NULL AS isError,
  totalSlotMs,
  totalSlotMs / runtimeMs AS avgSlots,
  /*
   * The following statement breaks down the query into minute buckets
   * and provides the average slot usage within that minute. This is a
   * crude way of making it so you can retrieve the average slot utilization
   * for a particular minute across multiple queries.
   */
  ARRAY(
    SELECT
      STRUCT(
        TIMESTAMP_TRUNC(
          TIMESTAMP_ADD(startTime, INTERVAL bucket_num MINUTE), MINUTE
        ) AS time,
        totalSlotMs / runtimeMs AS avgSlotUsage
      )
    FROM UNNEST(GENERATE_ARRAY(1, executionMinuteBuckets)) AS bucket_num
  ) AS executionTimeline,
  totalTablesProcessed,
  totalViewsProcessed,
  totalProcessedBytes,
  totalBilledBytes,
  (totalBilledBytes / pow(2,30)) AS totalBilledGigabytes,
  (totalBilledBytes / pow(2,40)) AS totalBilledTerabytes,
  (totalBilledBytes / pow(2,40)) * 5 AS estimatedCostUsd,
  billingTier,
  CONCAT(querydestTable_dataset_id, '.', querydestTable_table_id) AS queryDestinationTableRelativePath,
  CONCAT(querydestTable_project_id, '.', querydestTable_dataset_id, '.', querydestTable_table_id) AS queryDestinationTableAbsolutePath,
  referencedViews,
  referencedTables,
  referencedRoutines,
  /*
   * Load job statistics
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load_1
  */
  loadStatsTotalOutputBytes AS totalLoadOutputBytes,
  (loadStatsTotalOutputBytes / pow(2,30)) AS totalLoadOutputGigabytes,
  (loadStatsTotalOutputBytes / pow(2,40)) AS totalLoadOutputTerabytes,
  /*
   * loadConfig STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load
   */
  STRUCT(
    loadsourceUris,
    loadsourceUrisTruncated,
    loadcreateDisposition,
    loadwriteDisposition,
    loadschemaJson,
    loadschemaJsonTruncated,
    loadDestinationTable,
    STRUCT(
      loadKmskeyName
    ) AS destinationTableEncryption
  ) AS loadConfig,
  /*
   * queryConfig STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#query
   */
  STRUCT(
    query,
    queryTruncated,
    querycreateDisposition,
    querywriteDisposition,
    queryschemaJson,
    queryschemaJsonTruncated,
    querydestTable_project_id,
    querydestTable_dataset_id,
    querydestTable_table_id,
    queryPriority,
    querydefaultDataset,
    STRUCT(queryKmskeyName AS kmsKeyName) AS destinationTableEncryption
  ) AS queryConfig,
  /*
   * extractConfig STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#extract
   */
  STRUCT(
    extractdestinationUris,
    extractdestinationUrisTruncated,
    STRUCT(
      extract_projectid,
      extract_datasetid,
      extract_tableid,
      CONCAT(extract_datasetid, '.', extract_tableid) AS relativeTableRef,
      CONCAT(extract_projectid, '.', extract_datasetid, '.', extract_tableid) AS absoluteTableRef
    ) AS sourceTable
  ) AS extractConfig,
  /*
   * tableCopyConfig STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablecopy
   */
  STRUCT(
    tableCopysourceTables,
    tableCopysourceTablesTruncated,
    tableCopycreateDisposition,
    tableCopywriteDisposition,
    STRUCT(
      tableCopyproject_id,
      tableCopydataset_id,
      tableCopytable_id,
      CONCAT(tableCopydataset_id, '.', tableCopytable_id) AS relativeTableRef,
      CONCAT(tableCopyproject_id, '.', tableCopydataset_id, '.', tableCopytable_id) AS absoluteTableRef
    ) AS destinationTable,
    STRUCT(
      tableCopykmsKeyname
    ) AS destinationTableEncryption
  ) AS tableCopyConfig,
  /*
   * JobStatus STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#JobStatus
   */
  STRUCT(
    jobState,
    STRUCT (
      errorResultCode,
      errorResultMessage,
      errorResultDetails
    ) AS errorResult,
    STRUCT (
      errorCode,
      errorMessage
    ) AS error
  ) AS JobStatus,
  /*
   * JobStats STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstats
   */
  STRUCT(
    createTime,
    startTime,
    endTime,
    totalSlotMs,
    STRUCT(reservationUsageName AS name, reservationUsageSlotMs AS slotMs) AS reservationUsage,
    STRUCT (
      totalProcessedBytes,
      totalBilledBytes,
      billingTier,
      referencedViews,
      referencedTables,
      referencedRoutines,
      cacheHit,
      outputRowCount
    ) AS queryStats,
    STRUCT (
      loadStatsTotalOutputBytes AS totalOutputBytes
    ) AS loadStats
  ) AS jobStats,
  /*
   * ModelDeletion STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modeldeletion
   */
   STRUCT(
     modelDeletionReason AS reason
   ) AS modelDeletion,
   /*
   * ModelCreation STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelcreation
   */
   STRUCT(
     modelCreationReason AS reason,
     STRUCT(
       modelCreationModelName AS modelName,
       STRUCT(
         modelCreationEntityInfoFriendlyName AS friendlyName,
         modelCreationEntityInfoDescription AS description,
         modelCreationEntityInfoLabels AS labels
       ) AS entityInfo,
       modelCreationModelExpireTime AS expireTime,
       modelCreationModelCreateTime AS createTime,
       modelCreationModelUpdateTime AS updateTime,
       STRUCT(modelCreationEncryptionKmsKeyName AS kmsKeyName) AS encryptionInfo
     ) AS model
   ) AS modelCreation,
   /*
   * ModelMetadataChange STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelmetadatachange
   */
   STRUCT(
     modelMetadataChangeReason AS reason,
     STRUCT(
       modelMetadataChangeModelName AS modelName,
       STRUCT(
         modelMetadataChangeEntityInfoFriendlyName AS friendlyName,
         modelMetadataChangeEntityInfoDescription AS description,
         modelMetadataChangeEntityInfoLabels AS labels
       ) AS entityInfo,
       modelMetadataChangeModelExpireTime AS expireTime,
       modelMetadataChangeModelCreateTime AS createTime,
       modelMetadataChangeModelUpdateTime AS updateTime,
       STRUCT(modelMetadataChangeEncryptionKmsKeyName AS kmsKeyName) AS encryptionInfo
     ) AS model
   ) AS modelMetadataChange

FROM query_audit
LEFT JOIN data_audit USING(jobId)
LEFT JOIN table_audit USING(jobId)
LEFT JOIN tableDeletion_audit USING(jobId)
LEFT JOIN tableDataRead_audit USING(jobId)
LEFT JOIN modelDeletion_audit USING(jobId)
LEFT JOIN modelMetadataChange_audit USING(jobId)
LEFT JOIN modelCreation_audit USING(jobId)
LEFT JOIN modelDataChange_audit USING(jobId)
LEFT JOIN routine_audit USING(jobId)
WHERE
  statementType = "SCRIPT"
  OR jobChangeAfter = "DONE"
  OR tableDataChangeReason = "QUERY"
// Reference infrastructure for the telemetry ingestion pipeline.
// Deploy:  az deployment group create -g <resource-group> -f infra/main.bicep
//
// Uses connection strings for simplicity. For production, switch to managed identity
// (see "Production checklist" in the README).

@description('Short prefix for resource names (lowercase letters and numbers, 3-11 chars).')
@minLength(3)
@maxLength(11)
param prefix string = 'telem'

param location string = resourceGroup().location

@description('Autoscale ceiling in RU/s for the telemetry container. It scales down to 10% of this when idle.')
param maxThroughput int = 4000

@description('Event Hubs partitions. Sets the maximum parallelism of the Functions consumer.')
param eventHubPartitions int = 8

var suffix = uniqueString(resourceGroup().id)
var names = {
  cosmos: '${prefix}-cosmos-${suffix}'
  eventHubs: '${prefix}-ehns-${suffix}'
  storage: take('${prefix}st${suffix}', 24)
  plan: '${prefix}-plan-${suffix}'
  functionApp: '${prefix}-func-${suffix}'
}

// ---------------- Cosmos DB ----------------
resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' = {
  name: names.cosmos
  location: location
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
  }
}

resource database 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-05-15' = {
  parent: cosmos
  name: 'telemetry'
  properties: {
    resource: {
      id: 'telemetry'
    }
  }
}

resource eventsContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  parent: database
  name: 'events'
  properties: {
    resource: {
      id: 'events'
      partitionKey: {
        paths: [
          '/pk'
        ]
        kind: 'Hash'
      }
      defaultTtl: 2592000 // 30 days
      // Write-heavy workload: index only what queries use. Every indexed path costs RUs on every write.
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [
          { path: '/deviceId/?' }
          { path: '/timestamp/?' }
          { path: '/metricName/?' }
        ]
        excludedPaths: [
          { path: '/*' }
        ]
      }
    }
    options: {
      autoscaleSettings: {
        maxThroughput: maxThroughput
      }
    }
  }
}

resource deadLetterContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  parent: database
  name: 'deadletters'
  properties: {
    resource: {
      id: 'deadletters'
      partitionKey: {
        paths: [
          '/pk'
        ]
        kind: 'Hash'
      }
      defaultTtl: 1209600 // 14 days
    }
    options: {
      throughput: 400
    }
  }
}

// ---------------- Event Hubs ----------------
resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: names.eventHubs
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
}

resource telemetryHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubsNamespace
  name: 'telemetry'
  properties: {
    partitionCount: eventHubPartitions
    messageRetentionInDays: 1
  }
}

resource consumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: telemetryHub
  name: 'ingestion'
}

resource listenRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = {
  parent: telemetryHub
  name: 'ingestion-listen'
  properties: {
    rights: [
      'Listen'
    ]
  }
}

// ---------------- Function App ----------------
resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: names.storage
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource plan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: names.plan
  location: location
  kind: 'linux'
  sku: {
    name: 'EP1'
    tier: 'ElasticPremium'
  }
  properties: {
    reserved: true
    maximumElasticWorkerCount: eventHubPartitions
  }
}

var storageConnection = 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}'

resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: names.functionApp
  location: location
  kind: 'functionapp,linux'
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'DOTNET-ISOLATED|8.0'
      minTlsVersion: '1.2'
      ftpsState: 'Disabled'
      appSettings: [
        { name: 'AzureWebJobsStorage', value: storageConnection }
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'dotnet-isolated' }
        { name: 'EventHubConnection', value: listenRule.listKeys().primaryConnectionString }
        { name: 'EventHubName', value: telemetryHub.name }
        { name: 'EventHubConsumerGroup', value: consumerGroup.name }
        { name: 'CosmosConnection', value: cosmos.listConnectionStrings().connectionStrings[0].connectionString }
        { name: 'CosmosDatabase', value: database.name }
        { name: 'CosmosContainer', value: eventsContainer.name }
        { name: 'CosmosDeadLetterContainer', value: deadLetterContainer.name }
        { name: 'PartitionBucketMinutes', value: '60' }
        { name: 'PartitionShardCount', value: '8' }
        { name: 'MaxWriteConcurrency', value: '32' }
        { name: 'MaxWriteAttempts', value: '8' }
      ]
    }
  }
}

output functionAppName string = functionApp.name
output eventHubsNamespace string = eventHubsNamespace.name
output cosmosAccount string = cosmos.name

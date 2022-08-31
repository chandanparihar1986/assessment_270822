select * from
 (
     select
         eventData.geoFenceCollection as geoFenceCollection, eventData.geoFenceName as geoFenceName, eventData.geofence.featureCollectionId as gf_featureCollectionId,
         eventData.geofence.featureId as gf_featureId, eventData.geofence.properties.address as gf_address, eventData.geofence.properties.address2 as gf_address2,
         eventData.geofence.properties.address3 as gf_address3, eventData.geofence.properties.centroid as gf_centroid, eventData.geofence.properties.comment as gf_comment,
         eventData.geofence.properties.createdBy as gf_createdBy, cast(replace(eventData.geofence.properties.createdTimestamp,'T',' ') as timestamp) as gf_createdTimestamp,
         eventData.geofence.properties.lastModifiedBy as gf_lastModifiedBy, cast(replace(eventData.geofence.properties.lastModifiedTimestamp,'T',' ') as timestamp) as gf_lastModifiedTimestamp,
         eventData.geofence.properties.name as gf_name, eventData.geofence.properties.pool as gf_pool,
         case when nvl(eventData.latitude,'')<>'' then eventData.latitude else eventData.location.latitude end as latitude,
         case when nvl(eventData.longitude,'')<>'' then eventData.longitude else eventData.location.longitude end as longitude, eventData.sensor as sensor,
         eventData.sensorData.batteryLevel as sensor_batteryLevel, eventData.sensorData.batteryRange as sensor_batteryRange,
         eventData.sensorData.evBatteryLevel as sensor_evBatteryLevel, eventData.sensorData.fuelAmount as sensor_fuelAmount,
         eventData.sensorData.fuelLevel as sonsor_fuelLevel, eventData.sensorData.mileage as sonsor_mileage,
         eventData.sensorData.remainingLife as sensor_remainingLife, eventData.sensorData.status as sensor_status,
         eventData.sensorData.wheels.axle as sensor_whl_axle,  eventData.sensorData.wheels.position as sensor_whl_position,
         eventData.sensorData.wheels.pressure as sensor_whl_pressure, eventData.sensorData.wheels.side as sensor_whl_side,
         eventData.sensorData.wheels.uom as sensor_whl_uom,
         eventId, geofenceAlertTS, case when nvl(subType,'')<>'' then subType else subtype end as subType,
         cast(replace(timestamp,'T',' ') as timestamp) event_time, type, vehicleId
     from event_data
 )x
 where geoFenceCollection is not null

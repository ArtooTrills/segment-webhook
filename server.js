const express = require('express')
const bodyParser = require('body-parser');
const uuidv4 = require('uuid/v4');

const app = express()

let flattenObject = function(ob) {
	var toReturn = {};

	for (var i in ob) {
		if (!ob.hasOwnProperty(i)) continue;

		if ((typeof ob[i]) == 'object') {
			var flatObject = flattenObject(ob[i]);
			for (var x in flatObject) {
				if (!flatObject.hasOwnProperty(x)) continue;
				toReturn[i + '_' + x] = flatObject[x];
			}
		} else {
			toReturn[i] = ob[i];
		}
	}
	return toReturn;
};

let getQueries = function(uuid, data) {
    switch(data.type) {
        case "identify":
            return getIdentifyQuery(uuid, data);

        case "screen":
            return getScreenQuery(uuid, data);

        case "track":
            return getTrackQuery(uuid, data);

        default:
            return [];
    }
}

const { Pool, Client } = require('pg')
const pool = new Pool()

app.use(bodyParser.json());
app.get('/', (req, res) => {
    res.send('Hello Segment!');
});

app.get('/now', (req, res) => {
    let now = "";
    pool.query("SELECT NOW()", (err, result) => {
        if(err) {
            return res.status(403).send(err);
        }
        now = result.rows[0].now;
        return res.status(200).json({'now': now});
    });
});

let getScreenQuery = function(uuid, data) {
    return [{
        text: 'INSERT INTO android_dev.screens(id, received_at, context_app_namespace, context_app_version, context_device_advertising_id, context_screen_height, timestamp, context_locale, context_network_bluetooth, context_os_version, context_screen_density, context_device_manufacturer, context_traits_org, context_device_type, context_ip, context_network_wifi, context_traits_anonymous_id, context_user_agent, context_library_version, document_id, original_timestamp, context_device_id, context_device_model, context_library_name, context_network_cellular, context_timezone, context_traits_role, anonymous_id, context_os_name, context_screen_width, context_traits_context, context_traits_name, sent_at, context_app_build, context_app_name, context_device_ad_tracking_enabled, context_device_name, name) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38)',
        values: [uuid, data.receivedAt, data.context_app_namespace, data.context_app_version, data.context_device_advertisingId, data.context_screen_height, data.timestamp, data.context_locale, data.context_network_bluetooth, data.context_os_version, Math.round(data.context_screen_density), data.context_device_manufacturer, data.context_traits_org, data.context_device_type, data.context_ip, data.context_network_wifi, data.context_traits_anonymousId, data.context_userAgent, data.context_library_version, data.properties_documentId, data.originalTimestamp, data.context_device_id, data.context_device_model, data.context_library_name, data.context_network_cellular, data.context_timezone, data.context_traits_role, data.anonymousId, data.context_os_name, data.context_screen_width, data.context_traits_context, data.context_traits_name, data.sentAt, data.context_app_build, data.context_app_name, data.context_device_adTrackingEnabled, data.context_device_name, data.name],
      }]
}  

let getIdentifyQuery = function(uuid, data) {
    return [{
        text: 'INSERT INTO android_dev.identifies(id, received_at, context_app_name, context_network_wifi, context_screen_density, context_timezone, context_traits_anonymous_id, context_app_build, context_device_model, context_device_type, context_library_version, context_traits_org, context_traits_role, org, timestamp, context_library_name, context_network_carrier, context_network_cellular, context_screen_height, context_os_version, context_screen_width, context_traits_context, context_app_namespace, context_device_id, context_ip, context_os_name, role, context_device_manufacturer, name, original_timestamp, context_locale, context_traits_name, context_network_bluetooth, anonymous_id, context, context_device_ad_tracking_enabled, context_device_advertising_id, sent_at, context_app_version, context_device_name, context_user_agent) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41) RETURNING *',
        values: [uuid, data.receivedAt, data.context_app_name, data.context_network_wifi, Math.round(data.context_screen_density), data.context_timezone, data.context_traits_anonymousId, data.context_app_build, data.context_device_model, data.context_device_type, data.context_library_version, data.context_traits_org, data.context_traits_role, data.traits_org, data.timestamp, data.context_library_name, data.context_network_carrier, data.context_network_cellular, data.context_screen_height, data.context_os_version, data.context_screen_width, data.context_traits_context, data.context_app_namespace, data.context_device_id, data.context_ip, data.context_os_name, data.traits_role, data.context_device_manufacturer, data.traits_name, data.originalTimestamp, data.context_locale, data.context_traits_name, data.context_network_bluetooth, data.traits_anonymousId, data.traits_context, data.context_device_adTrackingEnabled, data.context_device_advertisingId, data.sentAt, data.context_app_version, data.context_device_name, data.context_userAgent],
    }]
};

let getTrackQuery = function(uuid, data) {
    let query = []
    query.push({
        text: 'INSERT INTO android_dev.tracks(id, received_at, context_app_namespace, context_device_type, context_ip, context_library_name, original_timestamp, sent_at, context_app_build, context_locale, context_screen_density, context_traits_role, event_text, context_app_name, context_device_ad_tracking_enabled, context_device_advertising_id, context_device_name, context_screen_width, timestamp, context_app_version, context_device_id, context_network_bluetooth, context_screen_height, anonymous_id, context_device_model, context_network_wifi, context_os_name, context_user_agent, context_os_version, context_traits_org, event, context_device_manufacturer, context_library_version, context_network_cellular, context_timezone, context_traits_anonymous_id, context_traits_context, context_traits_name) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38)',
        values: [uuid, data.receivedAt, data.context_app_namespace, data.context_device_type, data.context_ip, data.context_library_name, data.originalTimestamp, data.sentAt, data.context_app_build, data.context_locale, Math.round(data.context_screen_density), data.context_traits_role, data.event_text, data.context_app_name, data.context_device_adTrackingEnabled, data.context_device_advertisingId, data.context_device_name, data.context_screen_width, data.timestamp, data.context_app_version, data.context_device_id, data.context_network_bluetooth, data.context_screen_height, data.anonymousId, data.context_device_model, data.context_network_wifi, data.context_os_name, data.context_user_agent, data.context_os_version, data.context_traits_org, data.event, data.context_device_manufacturer, data.context_library_version, data.context_network_cellular, data.context_timezone, data.context_traits_anonymousId, data.context_traits_context, data.context_traits_name],
    });
    query.push(getEventQuery(uuid, data));
    return query.filter(item => item != "");
};

let getEventQuery = (uuid, data)=> {
    switch(data.event){
        case "Application Installed":
            return {
                text: 'INSERT INTO android_dev.application_installed(id, received_at, context_app_name, context_device_model, context_traits_name, event, timestamp, anonymous_id, event_text, context_app_namespace, context_device_id, context_os_version, context_timezone, context_traits_anonymous_id, build, context_device_advertising_id, context_traits_org, context_traits_role, context_app_build, context_device_name, context_library_name, context_network_wifi, sent_at, context_device_ad_tracking_enabled, context_device_type, context_screen_density, context_screen_height, context_user_agent, version, context_app_version, context_locale, context_network_cellular, context_os_name, context_traits_context, original_timestamp, context_device_manufacturer, context_ip, context_library_version, context_network_bluetooth, context_screen_width) \
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)',
                values: [uuid, data.receivedAt, data.context_app_name, data.context_device_model, data.context_traits_name, data.event, data.timestamp, data.anonymousId, data.event, data.context_app_namespace, data.context_device_id, data.context_os_version, data.context_timezone, data.context_traits_anonymousId, data.properties_build, data.context_device_advertisingId, data.context_traits_org, data.context_traits_role, data.context_app_build, data.context_device_name, data.context_library_name, data.context_network_wifi, data.sentAt, data.context_device_adTrackingEnabled, data.context_device_type, Math.round(data.context_screen_density), data.context_screen_height, data.context_userAgent, data.properties_version, data.context_app_version, data.context_locale, data.context_network_cellular, data.context_os_name, data.context_traits_context, data.originalTimestamp, data.context_device_manufacturer, data.context_ip, data.context_library_version, data.context_network_bluetooth, data.context_screen_width]
            }

        case "Application Opened":
            return {
                text: 'INSERT INTO android_dev.application_opened(id, received_at, anonymous_id, context_device_id, context_screen_height, context_screen_width, context_traits_name, sent_at, version, context_library_version, context_screen_density, context_traits_org, context_app_namespace, context_app_version, context_traits_context, context_app_build, context_device_model, context_locale, context_network_wifi, context_timezone, context_user_agent, context_device_type, context_ip, event_text, build, context_app_name, context_device_manufacturer, context_network_bluetooth, context_network_cellular, context_os_name, context_os_version, timestamp, context_device_advertising_id, context_library_name, context_device_ad_tracking_enabled, context_device_name, context_traits_anonymous_id, context_traits_role, event, original_timestamp) \
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)',
                values: [uuid, data.receivedAt, data.anonymousId, data.context_device_id, data.context_screen_height, data.context_screen_width, data.context_traits_name, data.sentAt, data.properties_version, data.context_library_version, Math.round(data.context_screen_density), data.context_traits_org, data.context_app_namespace, data.context_app_version, data.context_traits_context, data.context_app_build, data.context_device_model, data.context_locale, data.context_network_wifi, data.context_timezone, data.context_user_agent, data.context_device_type, data.context_ip, data.event, data.properties_build, data.context_app_name, data.context_device_manufacturer, data.context_network_bluetooth, data.context_network_cellular, data.context_os_name, data.context_os_version, data.timestamp, data.context_device_advertisingId, data.context_library_name, data.context_device_adTrackingEnabled, data.context_device_name, data.context_traits_anonymousId, data.context_traits_role, data.event, data.originalTimestamp],
            }

        case "Value_Stream":
            return {
                text: 'INSERT INTO android_dev.value_stream(id, received_at, _tag, context_app_build, context_device_type, value, context_device_ad_tracking_enabled, context_network_cellular, context_traits_context, old_value, path, timestamp, context_ip, context_library_name, context_os_version, context_timezone, context_user_agent, original_timestamp, context_app_name, context_device_advertising_id, context_screen_width, context_traits_org, document_id, context_app_namespace, context_device_name, context_network_wifi, context_os_name, sent_at, context_device_id, event, event_text, context_screen_height, type, anonymous_id, context_app_version, context_device_model, context_library_version, context_screen_density, context_traits_role, context_device_manufacturer, context_locale, context_network_bluetooth, context_traits_anonymous_id, context_traits_name) \
                VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44)',
                values: [uuid, data.receivedAt, data.properties_tag, data.context_app_build, data.context_device_type, data.properties_value, data.context_device_adTrackingEnabled, data.context_network_cellular, data.context_traits_context, data.properties_old_value, data.properties_path, data.timestamp, data.context_ip, data.context_library_name, data.context_os_version, data.context_timezone, data.context_userAgent, data.originalTimestamp, data.context_app_name, data.context_device_advertisingId, data.context_screen_width, data.context_traits_org, data.properties_documentId, data.context_app_namespace, data.context_device_name, data.context_network_wifi, data.context_os_name, data.sentAt, data.context_device_id, data.event, data.event, data.context_screen_height, data.properties_type, data.anonymousId, data.context_app_version, data.context_device_model, data.context_library_version, Math.round(data.context_screen_density), data.context_traits_role, data.context_device_manufacturer, data.context_locale, data.context_network_bluetooth, data.context_traits_anonymousId, data.context_traits_name]
            }

        default:
            return null;
    }
}

app.post('/track', (req, res) => {
    let request_data = flattenObject(req.body);
    let uuid = uuidv4();
    let queries = getQueries(uuid, request_data);

    console.log(request_data.type, request_data);
    let runnable_queries = []
    queries.forEach(query => {
        runnable_queries.push(pool.query(query.text, query.values))
    });
    let results = []
    runnable_queries.forEach(promise=> {
        results.push(promise.catch(error =>{
            return error;
        }));
    });

    Promise.all(results)
    .then(result => {
        return res.sendStatus(200);
    })
    .catch(error => {
        console.log(error);
        return res.status(500).send(error);
    })
});

app.listen(3000, () => console.log('Example app listening on port 3000!'))

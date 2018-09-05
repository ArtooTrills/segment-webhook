const express = require('express')
const bodyParser = require('body-parser');
const uuidv3 = require('uuid/v3');

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
    return {
        text: 'INSERT INTO android_dev.screens(id, received_at, context_app_namespace, context_app_version, context_device_advertising_id, context_screen_height, timestamp, context_locale, context_network_bluetooth, context_os_version, context_screen_density, context_device_manufacturer, context_traits_org, context_device_type, context_ip, context_network_wifi, context_traits_anonymous_id, context_user_agent, context_library_version, document_id, original_timestamp, context_device_id, context_device_model, context_library_name, context_network_cellular, context_timezone, context_traits_role, anonymous_id, context_os_name, context_screen_width, context_traits_context, context_traits_name, sent_at, context_app_build, context_app_name, context_device_ad_tracking_enabled, context_device_name, name) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38)',
        values: [uuid, data.receivedAt, data.context_app_namespace, data.context_app_version, data.context_device_advertisingId, data.context_screen_height, data.timestamp, data.context_locale, data.context_network_bluetooth, data.context_os_version, data.context_screen_density, data.context_device_manufacturer, data.context_traits_org, data.context_device_type, data.context_ip, data.context_network_wifi, data.context_traits_anonymousId, data.context_userAgent, data.context_library_version, data.properties_documentId, data.originalTimestamp, data.context_device_id, data.context_device_model, data.context_library_name, data.context_network_cellular, data.context_timezone, data.context_traits_role, data.anonymousId, data.context_os_name, data.context_screen_width, data.context_traits_context, data.context_traits_name, data.sentAt, data.context_app_build, data.context_app_name, data.context_device_adTrackingEnabled, data.context_device_name, data.name],
      }
}  

let getIdentifyQuery = function(uuid, data) {
    return {
        text: 'INSERT INTO android_dev.identifies(received_at, context_app_name, context_network_wifi, context_screen_density, context_timezone, context_traits_anonymous_id, context_app_build, context_device_model, context_device_type, context_library_version, context_traits_org, context_traits_role, org, timestamp, context_library_name, context_network_carrier, context_network_cellular, context_screen_height, context_os_version, context_screen_width, context_traits_context, context_app_namespace, context_device_id, context_ip, context_os_name, role, context_device_manufacturer, name, original_timestamp, context_locale, context_traits_name, context_network_bluetooth, anonymous_id, context, context_device_ad_tracking_enabled, context_device_advertising_id, sent_at, context_app_version, context_device_name, context_user_agent) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41) RETURNING *',
        values: [uuid, data.receivedAt, data.context_app_name, data.context_network_wifi, data.context_screen_density, data.context_timezone, data.context_traits_anonymousId, data.context_app_build, data.context_device_model, data.context_device_type, data.context_library_version, data.context_traits_org, data.context_traits_role, data.traits_org, data.timestamp, data.context_library_name, data.context_network_carrier, data.context_network_cellular, data.context_screen_height, data.context_os_version, data.context_screen_width, data.context_traits_context, data.context_app_namespace, data.context_device_id, data.context_ip, data.context_os_name, data.traits_role, data.context_device_manufacturer, data.traits_name, data.originalTimestamp, data.context_locale, data.context_traits_name, data.context_network_bluetooth, data.traits_anonymousId, data.traits_context, data.context_device_adTrackingEnabled, data.context_device_advertisingId, data.sentAt, data.context_app_version, data.context_device_name, data.context_userAgent],
    }
};

let getTrackQuery = function(uuid, data) {
    // all fields are required
    return {
        text: 'INSERT INTO android_dev.tracks(id, received_at, context_app_namespace, context_device_type, context_ip, context_library_name, original_timestamp, sent_at, context_app_build, context_locale, context_screen_density, context_traits_role, event_text, context_app_name, context_device_ad_tracking_enabled, context_device_advertising_id, context_device_name, context_screen_width, timestamp, context_app_version, context_device_id, context_network_bluetooth, context_screen_height, anonymous_id, context_device_model, context_network_wifi, context_os_name, context_user_agent, context_os_version, context_traits_org, event, context_device_manufacturer, context_library_version, context_network_cellular, context_timezone, context_traits_anonymous_id, context_traits_context, context_traits_name) \
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38)',
        values: [uuid, data.receivedAt, data.context_app_namespace, data.context_device_type, data.context_ip, data.context_library_name, data.originalTimestamp, data.sentAt, data.context_app_build, data.context_locale, data.context_screen_density, data.context_traits_role, data.event_text, data.context_app_name, data.context_device_adTrackingEnabled, data.context_device_advertisingId, data.context_device_name, data.context_screen_width, data.timestamp, data.context_app_version, data.context_device_id, data.context_network_bluetooth, data.context_screen_height, data.anonymousId, data.context_device_model, data.context_network_wifi, data.context_os_name, data.context_user_agent, data.context_os_version, data.context_traits_org, data.event, data.context_device_manufacturer, data.context_library_version, data.context_network_cellular, data.context_timezone, data.context_traits_anonymousId, data.context_traits_context, data.context_traits_name],
    }
};
app.post('/track', (req, res) => {
    let reqData = flattenObject(req.body);
    let query = '';
    let uuid = uuidv3('http://api.artoo.in', uuidv3.URL);
    switch(reqData.type) {
        case "identify":
            query = getIdentifyQuery(uuid, reqData)
        break;
        // case "page":
        //     query = getPageQuery(reqData)
        // break;
        case "screen":
            query = getScreenQuery(uuid, reqData)
        break;
        case "track":
            query = getTrackQuery(uuid, reqData)
        break;
        default:
            return res.send(403).json({'message': 'Bad payload type:' + reqData.type});
    }
    console.log(reqData.type, reqData);
    pool.query(query.text, query.values, (err, res) => {
        if(err) {
            return res.sendStatus(500);
        }

        return res.sendStatus(200);
    });
});

app.listen(3000, () => console.log('Example app listening on port 3000!'))

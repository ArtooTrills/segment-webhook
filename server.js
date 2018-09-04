const express = require('express')
let bodyParser = require('body-parser');

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
    res.send('Hello Segment!?');
});

app.get('/page', (req, res) => {
    let now = "";
    pool.query('SELECT NOW()', (err, result) => {
        if(err) {
            return res.sendStatus(403);
        }
        now = result.rows[0].now;
        return res.status(200).json({'now': now})
    });
});

let getPageQuery = function(data) {
    const pageQuery = {
        text: 'INSERT INTO users(name, email) VALUES($1, $2)',
        values: ['brianc', 'brian.m.carlson@gmail.com'],
      }
}

let getScreenQuery = function(data) {
    const screenQuery = {
        text: 'INSERT INTO users(name, email) VALUES($1, $2)',
        values: ['brianc', 'brian.m.carlson@gmail.com'],
      }
}  

let getIdentifyQuery = function(data) {
    const identifyQuery = {
        text: 'INSERT INTO users(name, email) VALUES($1, $2)',
        values: ['brianc', 'brian.m.carlson@gmail.com'],
    }
};

let getTrackQuery = function(data) {
    // all fields are required
    const trackQuery = {
        text: 'INSERT INTO tracks(name, email) VALUES($1, $2)',
        values: ['brianc', 'brian.m.carlson@gmail.com'],
      }
};
app.post('/track', (req, res) => {
    let reqData = flattenObject(req.body);
    let query = '';
    switch(reqData.type) {
        case "identify":
            query = getIdentifyQuery(reqData)
        break;
        case "page":
            query = getPageQuery(reqData)
        break;
        case "screen":
            query = getScreenQuery(reqData)
        break;
        case "track":
            query = getTrackQuery(reqData)
        break;
        default:
            return res.send(403).json({'message': 'Bad payload type:' + reqData.type});
    }
    console.log(track);
    pool.query('', (err, res) => {
        if(err) {
            return res.sendStatus(500);
        }


        return res.sendStatus(200);
    });
});

app.listen(3000, () => console.log('Example app listening on port 3000!'))

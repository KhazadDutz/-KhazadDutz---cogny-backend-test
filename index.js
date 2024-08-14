const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const axios = require('axios');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            // await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };


    //Função para consumir a api
    async function fetchData(url) {
        try {
            const response = await axios.get(url);
            const { data } = response.data;

            return data;
        } catch (error) {
            console.error('Erro ao consumir a API:', error);
        }
    }

    //Função para salvar os dados no database
    async function saveData(data) {
        try {
            await db[DATABASE_SCHEMA].api_data.insert({
                doc_record: JSON.stringify(data),
            })

            console.log('Dados inseridos com sucesso!');
        } catch (error) {
            console.error('Erro ao salvar os dados:', error);
        }
    }


    try {
        const url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";
        
        //Consumo da API;
        const payload = await fetchData(url);
        
        //Execução das migrations;
        await migrationUp();

        //Salva os dados no DB;
        await saveData(payload);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();
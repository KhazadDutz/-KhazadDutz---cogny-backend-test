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
    async function saveDataToDB(data) {
        try {
            await db[DATABASE_SCHEMA].api_data.insert({
                doc_record: JSON.stringify(data),
            })

            console.log('Dados inseridos com sucesso!');
        } catch (error) {
            console.error('Erro ao salvar os dados:', error);
        }
    }

    // Função para calcular a soma da população em memória
    async function calculatePopulationInMemory(data) {
        try {
            const payload = data;
            const years = ['2020', '2019', '2018'];
            const totalPopulation = payload
                .filter(item => years.includes(item.Year))
                .reduce((total, item) => total + item.Population, 0); 
            
            console.log('Soma da população (em memória):', totalPopulation);
        } catch (error) {
            console.error('Erro ao calcular a população em memória:', error);
        }
    }

    //Função para criar a VIEW
    async function createView() {
        try {
            await db.query(`
                CREATE OR REPLACE VIEW ${DATABASE_SCHEMA}.api_data_view AS
                SELECT
                    (jsonb_array_elements(doc_record) -> 'Nation')::TEXT AS nation_name,
                    (jsonb_array_elements(doc_record) -> 'Year')::TEXT AS current_year,
                    (jsonb_array_elements(doc_record) -> 'Population')::INT AS nation_population
                FROM ${DATABASE_SCHEMA}.api_data;
            `);
            console.log('VIEW criada com sucesso!');
        } catch (error) {
            console.error('Erro ao criar a VIEW:', error);
        }
    }
    
    try {
        //URL da api a consumir
        const url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population";
        
        //Consumo da API;
        const payload = await fetchData(url);
        
        //Execução das migrations;
        await migrationUp();

        //Cálculo em memória;
        await calculatePopulationInMemory(payload);

        //Salva os dados no DB;
        await saveDataToDB(payload);

        //Cria uma View no DB;
        await createView();

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();
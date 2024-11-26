const fs = require('fs');
const { exporter } = require('@dbml/core');

// get DBML file content
const dbml = fs.readFileSync('./schema.dbml', 'utf-8');

const json = exporter.export(dbml, 'json');

fs.writeFile('./schema.json', json, { encoding: 'utf-8' }, (err) => {
    if (err) console.log(err);
});

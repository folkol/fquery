import fs from 'node:fs';
import * as Arrow from 'apache-arrow';

let impressions = [1, 2, 3, 4, 5, 100, 200, 300];
let clicks = impressions.map(impression => impression * 100);
let table = Arrow.tableFromArrays({
    impressions,
    clicks
});
let data = Arrow.tableToIPC(table);
fs.writeFileSync('data.arrow', data);

let table2 = Arrow.tableFromIPC(fs.readFileSync('data.arrow'));
console.log(JSON.stringify(table2));

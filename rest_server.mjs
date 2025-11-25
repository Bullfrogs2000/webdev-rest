import * as path from 'node:path';
import * as url from 'node:url';

import { default as express } from 'express';
import { default as sqlite3 } from 'sqlite3';

const __dirname = path.dirname(url.fileURLToPath(import.meta.url));
// Database should live in ./db/stpaul_crime.sqlite3 relative to this file
const db_filename = path.join(__dirname, 'db', 'stpaul_crime.sqlite3');

const port = 8000;

let app = express();
app.use(express.json());

/********************************************************************
 ***   DATABASE FUNCTIONS                                         *** 
 ********************************************************************/
// Open SQLite3 database (in read-write mode)
let db = new sqlite3.Database(db_filename, sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
        console.log('Error opening ' + path.basename(db_filename));
        console.log(err.message);
    }
    else {
        console.log('Now connected to ' + path.basename(db_filename));
    }
});

// Create Promise for SQLite3 database SELECT query 
function dbSelect(query, params=[]) {
    return new Promise((resolve, reject) => {
        db.all(query, params, (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
        });
    });
}

// Create Promise for SQLite3 database INSERT or DELETE query
function dbRun(query, params=[]) {
    return new Promise((resolve, reject) => {
        db.run(query, params, function (err) {
            if (err) reject(err);
            else resolve(this); // return statement context (changes, lastID)
        });
    });
}

/********************************************************************
 ***   HELPERS                                                   *** 
 ********************************************************************/
function parseCommaList(value, asNumber=false) {
    if (!value) return null;
    const items = String(value)
        .split(',')
        .map(s => s.trim())
        .filter(s => s.length > 0);
    if (items.length === 0) return null;
    if (!asNumber) return items;
    const nums = items.map(n => Number(n)).filter(n => Number.isFinite(n));
    return nums.length ? nums : null;
}

function inClause(field, values) {
    // returns { sql: "field IN (?,?,?)", params:[...] } or {sql:"",params:[]}
    if (!values || values.length === 0) return { sql: '', params: [] };
    const placeholders = values.map(() => '?').join(',');
    return { sql: `${field} IN (${placeholders})`, params: values };
}

/********************************************************************
 ***   REST REQUEST HANDLERS                                      *** 
 ********************************************************************/
// GET request handler for crime codes
// Optional query: ?code=110,700
app.get('/codes', async (req, res) => {
    try {
        const codes = parseCommaList(req.query.code, true);
        const where = inClause('code', codes);

        const sql = `
            SELECT code, incident_type AS type
            FROM Codes
            ${where.sql ? 'WHERE ' + where.sql : ''}
            ORDER BY code ASC
        `;
        const rows = await dbSelect(sql, where.params);
        res.status(200).json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).type('txt').send('Database error');
    }
});

// GET request handler for neighborhoods
// Optional query: ?id=11,14
app.get('/neighborhoods', async (req, res) => {
    try {
        const ids = parseCommaList(req.query.id, true);
        const where = inClause('neighborhood_number', ids);

        const sql = `
            SELECT neighborhood_number AS id,
                   neighborhood_name AS name
            FROM Neighborhoods
            ${where.sql ? 'WHERE ' + where.sql : ''}
            ORDER BY neighborhood_number ASC
        `;
        const rows = await dbSelect(sql, where.params);
        res.status(200).json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).type('txt').send('Database error');
    }
});

// GET request handler for new crime incidents
// Query options:
// start_date=YYYY-MM-DD
// end_date=YYYY-MM-DD
// code=...
// grid=...
// neighborhood=...
// limit=...
app.get('/incidents', async (req, res) => {
    try {
        const startDate = req.query.start_date ? String(req.query.start_date) : null;
        const endDate = req.query.end_date ? String(req.query.end_date) : null;

        const codes = parseCommaList(req.query.code, true);
        const grids = parseCommaList(req.query.grid, true);
        const neighborhoods = parseCommaList(req.query.neighborhood, true);

        let limit = Number(req.query.limit);
        if (!Number.isFinite(limit) || limit <= 0) limit = 1000;

        const clauses = [];
        const params = [];

        if (startDate) {
            clauses.push('date(date_time) >= date(?)');
            params.push(startDate);
        }
        if (endDate) {
            clauses.push('date(date_time) <= date(?)');
            params.push(endDate);
        }

        const codeClause = inClause('code', codes);
        if (codeClause.sql) {
            clauses.push(codeClause.sql);
            params.push(...codeClause.params);
        }

        const gridClause = inClause('police_grid', grids);
        if (gridClause.sql) {
            clauses.push(gridClause.sql);
            params.push(...gridClause.params);
        }

        const nClause = inClause('neighborhood_number', neighborhoods);
        if (nClause.sql) {
            clauses.push(nClause.sql);
            params.push(...nClause.params);
        }

        const whereSql = clauses.length ? 'WHERE ' + clauses.join(' AND ') : '';

        const sql = `
            SELECT case_number,
                   date(date_time) AS date,
                   time(date_time) AS time,
                   code,
                   incident,
                   police_grid,
                   neighborhood_number,
                   block
            FROM Incidents
            ${whereSql}
            ORDER BY datetime(date_time) DESC
            LIMIT ?
        `;

        const rows = await dbSelect(sql, [...params, limit]);
        res.status(200).json(rows);
    } catch (err) {
        console.error(err);
        res.status(500).type('txt').send('Database error');
    }
});

// PUT request handler for new crime incident
app.put('/new-incident', async (req, res) => {
    try {
        const {
            case_number,
            date,
            time,
            code,
            incident,
            police_grid,
            neighborhood_number,
            block
        } = req.body || {};

        if (!case_number || !date || !time) {
            return res.status(400).type('txt').send('Missing required fields');
        }

        // reject if case number already exists
        const existing = await dbSelect(
            'SELECT case_number FROM Incidents WHERE case_number = ?',
            [case_number]
        );
        if (existing.length > 0) {
            return res.status(500).type('txt').send('Case number already exists');
        }

        const date_time = `${date} ${time}`;

        await dbRun(
            `INSERT INTO Incidents
             (case_number, date_time, code, incident, police_grid, neighborhood_number, block)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
            [case_number, date_time, code, incident, police_grid, neighborhood_number, block]
        );

        res.status(200).type('txt').send('OK');
    } catch (err) {
        console.error(err);
        // primary key violation etc.
        res.status(500).type('txt').send('Insert failed');
    }
});

// DELETE request handler for removing crime incident
app.delete('/remove-incident', async (req, res) => {
    try {
        const { case_number } = req.body || {};
        if (!case_number) {
            return res.status(400).type('txt').send('Missing case_number');
        }

        const existing = await dbSelect(
            'SELECT case_number FROM Incidents WHERE case_number = ?',
            [case_number]
        );
        if (existing.length === 0) {
            return res.status(500).type('txt').send('Case number does not exist');
        }

        await dbRun('DELETE FROM Incidents WHERE case_number = ?', [case_number]);
        res.status(200).type('txt').send('OK');
    } catch (err) {
        console.error(err);
        res.status(500).type('txt').send('Delete failed');
    }
});

/********************************************************************
 ***   START SERVER                                               *** 
 ********************************************************************/
// Start server - listen for client connections
app.listen(port, () => {
    console.log('Now listening on port ' + port);
});

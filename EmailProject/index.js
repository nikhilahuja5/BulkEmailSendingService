import dotenv from 'dotenv';
import express from 'express';
import bodyParser from 'body-parser';
import {v4 as uuidv4} from 'uuid';
import pg from 'pg';
import QueueClient from './queue/queue.js';
dotenv.config();


const app = express();
const id = uuidv4();

app.use(bodyParser.json());

const pool = new pg.Pool({connectionString : process.env.DATABASE_URL});
const queue = QueueClient.getQueue();
const RETRY_INTERVAL = 60000;

async function retryFailedJobs() {
    try {
        const failedJobs = await queue.getFailed();
        if (failedJobs.length > 0) {
            console.log(`Retrying ${failedJobs.length} failed jobs...`);
            for (const job of failedJobs) {
                await job.retry();
                console.log(`Retried job ${job.id}`);
            }
        }
    } catch (err) {
        console.error('Failed to retry jobs:', err);
    }
}

const failedJobs = await queue.getFailed();
console.log('Failed jobs count:', failedJobs.length);

async function createSendRecord(client , subject , body , ownerId ,total){
    const res = await client.query(
         `INSERT INTO "emailschema".sends (id, owner_id, total, subject, body)
            VALUES ($1,$2,$3,$4,$5) RETURNING *`,
            [uuidv4(), ownerId, total, subject, body]
    );
    return res.rows[0];
}

async function insertEmailRows(client , sendId , recipients, subject , body){
    const insertText = `INSERT INTO "emailschema".emails (send_id, recipient_email, recipient_name, subject, body, status)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`;
    const rows = [];
    for(const r of recipients){
        const name = r.split('@')[0];

        const res = await client.query(insertText, [sendId, r , name ?? null , subject , body, 'queued']);
        rows.push({id : res.rows[0].id, recipient : {
            r,name
        } });
    }
    return rows;
}

app.post('/send', async(req,res) => {
    const {ownerId,subject,body,recipients} = req.body;
    console.log(req.body);
    
    if(!subject || !body || !Array.isArray(recipients) || recipients.length === 0){
        return res.status(400).json({ error: 'subject, body and recipients[] required' });
    }

    const client = await pool.connect();

    try{
        await client.query('BEGIN');

        const sendRecord = await createSendRecord(
            client,
            subject,
            body,
            ownerId,
            recipients.length
        );
        
        const sendId = sendRecord.id;
        console.log(sendId);

        const emailRows = await insertEmailRows(
            client,
            sendId,
            recipients,
            subject,
            body
        );

        console.log("emailRows",emailRows);

        for(const r of emailRows){
            await queue.add(
                'send-email', 
                {
                    emailId : r.id,
                    sendId, 
                    recipient:r.recipient,
                    subject,
                    body
                },
                {
                jobId : String(r.id),
                attempts : parseInt(process.env.MAX_RETRIES || '5'),
                backoff: {type: 'exponential' , delay : 1000 },
                removeOnComplete : true,
                removeOnFail : 100
            }
        );
    }

    await client.query('COMMIT');
    res.json({sendId,queued:emailRows.length});

    }catch (err){
        await client.query('ROLLBACK');
        console.error('Email Service Error', err);
        res.status(500).json({error : 'failed to enque'});
    } finally {
        client.release();
    }
});

app.get('/status/:sendId' , async (req,res) => {
    const {sendId} = req.params;
    const client = await pool.connect();
    try{
        const sendRes = await client.query('SELECT * FROM "emailschema".sends WHERE id=$1', [sendId]);
        if(sendRes.rowCount == 0)return res.status(404).json({ error: 'not found' });
        const emailRes = await client.query('SELECT id, recipient_email, status, attempt_count, last_error, message_id FROM "emailschema".emails WHERE send_id=$1', [sendId]);
        res.json({send:sendRes.rows[0] , items:emailRes.rows});
    } finally {
        client.release();
    }
});

// setInterval(retryFailedJobs, RETRY_INTERVAL);

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`server is listning at ${port}`));
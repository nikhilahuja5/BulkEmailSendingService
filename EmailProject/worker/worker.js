import dotenv from "dotenv";
import { Worker } from 'bullmq';
import  Redis from 'ioredis';
import  pg from 'pg';
import { sendMail, renderTemplate } from './mailer.js';

dotenv.config();

const connection = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');

const queueName = process.env.QUEUE_NAME || 'emails';

const pool = new pg.Pool({ connectionString : process.env.DATABASE_URL });

const concurrency = parseInt(process.env.CONCURRENCY || '5');
const rateLimitMax =  parseInt(process.env.RATE_LIMIT_MAX || '100');
const rateLimitDuration = parseInt(process.env.RATE_LIMIT_DURATION || '6000');

const worker = new Worker(queueName , async job => {
    const { emailId, recipient, subject, body } = job.data;
    const client = await pool.connect();
    try{
        await client.query('UPDATE "emailschema".emails SET status=$1, updated_at=now() WHERE id=$2', ['sending', emailId]);

        const html = renderTemplate(body, {name : recipient.name});
        const text = html.replace(/<[^>]*>/g, '');
        
        const info = await sendMail({to : recipient.email , toName: recipient.name , subject , html , text });
        console.log("INFO",info);
        await client.query(
            `UPDATE "emailschema".emails SET status=$1, attempt_count = attempt_count + 1, message_id=$2, updated_at=now() WHERE id=$3`,
            ['sent', info.messageId || null , emailId]
        );

        await client.query(`UPDATE "emailschema".sends SET sent_count = sent_count + 1, updated_at=now() WHERE id=$1`,[job.data.sendId]);

        return {ok : true};
    }catch(err){
        console.error('send error',err);

        await client.query(
                `UPDATE "emailschema".emails SET status=$1, attempt_count = attempt_count + 1, last_error=$2, updated_at=now() WHERE id=$3`,
                ['failed', String(err.message).slice(0, 1000), emailId]
        );
        
        await client.query(`UPDATE "emailschema".sends SET failed_count = failed_count + 1, updated_at=now() WHERE id=$1`, [job.data.sendId]);
        throw err;
    }finally{
        client.release();
    }
},{
    connection,
    concurrency,
    limiter : {
        max : rateLimitMax,
        duration : rateLimitDuration
    }
});

worker.on('completed', job => console.log(`Job ${job.id} completed`));
worker.on('failed', (job, err) => console.log(`Job ${job?.id} failed: ${err?.message}`));

console.log('Worker started, concurrency=', concurrency);
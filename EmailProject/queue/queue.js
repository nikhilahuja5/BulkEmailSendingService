import  {Queue} from 'bullmq';
import IORedis from 'ioredis';

let queue;

export default { 
    getQueue(){
        if(queue)return queue;
        const connection = new IORedis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');
        queue = new Queue(process.env.QUEUE_NAME || 'emails' , {connection});
        return queue;
    }
};
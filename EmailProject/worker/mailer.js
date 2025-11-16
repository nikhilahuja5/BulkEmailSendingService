import dotenv from "dotenv";
import nodemailer from 'nodemailer';
import handlebars from 'handlebars';

dotenv.config();

console.log("TYPE", typeof process.env.SMTP_PASS);

const transporter = nodemailer.createTransport(
    {
    host : process.env.SMTP_HOST,
    port : parseInt(process.env.SMTP_PORT || '587'),
    secure  : false,
    auth : {
        user : process.env.SMTP_USER,
        pass : process.env.SMTP_PASS
    }
})

export async function sendMail({to, toName , subject , html , text}) {
    const mail = {
        from : process.env.FROM_EMAIL || process.env.SMTP_USER,
        to : toName ? `${toName} <${to}>` : to,
        subject,
        html,
        text,
    }
    const info = await transporter.sendMail(mail);
    return info;
}

export function renderTemplate(templateString , data){
    const tpl = handlebars.compile(templateString);
    return tpl(data);
}


import json
import logging
from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
import datetime
from kafka import KafkaConsumer
# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)

logger = logging.getLogger(__name__)
kafka_consumer=None


def start(update: Update, context: CallbackContext) -> None:
    """Sends explanation on how to use the bot."""
    update.message.reply_text('Hi! Use /set <seconds> to set a timer')


def checking_status(context: CallbackContext) -> None:
    try:
        with open(run_file) as f:
            run_record = json.load(f)
            msg=\
            f"heartbeat: {run_record['heartbeat']}\nstatus: {run_record['status']}\n"
        import subprocess
        f = subprocess.Popen(['tail','-n 15',metrics_file],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        metrics=f.stdout.readlines()
        metrics="".join([str(each,'utf-8') for each in metrics])
        msg+=f"metrics: \n{metrics}"
    except Exception as e:
        msg = f"ERROR: {e}"
    job = context.job
    context.bot.send_message(job.context, text=msg)

def subscribe_topic(context: CallbackContext) -> None:
    msg=""
    global kafka_consumer
    try:
        for each in kafka_consumer:
            msg+=f"Time: {datetime.datetime.fromtimestamp(each.timestamp//1000).strftime('%Y-%m-%d %H:%M:%S')}\n{each.value.decode('utf-8')}\n\n"
    except Exception as e:
        msg = f"ERROR: {e}"
    if len(msg)>0:
        job = context.job
        context.bot.send_message(job.context, text=msg)


def remove_job_if_exists(name: str, context: CallbackContext) -> bool:
    """Remove job with given name. Returns whether job was removed."""
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
    return True


def set_timer(update: Update, context: CallbackContext) -> None:
    """Add a job to the queue."""
    chat_id = update.message.chat_id
    try:
        # args[0] should contain the time for the timer in seconds
        per = int(context.args[0])
        if per <= 0:
            update.message.reply_text('Invalid interval')
            return
        global kafka_consumer        
        kafka_consumer=KafkaConsumer(f'news-{context.args[1]}',bootstrap_servers='localhost:9092',consumer_timeout_ms=5000)
        job_removed = remove_job_if_exists(str(chat_id), context)
        context.job_queue.run_repeating(
            subscribe_topic,
            interval=datetime.timedelta(seconds=per),
            context=chat_id,
            name=str(chat_id))
        text = 'Job successfully set!'
        if job_removed:
            text += ' Old one was removed.'
        update.message.reply_text(text)

    except (IndexError, ValueError):
        update.message.reply_text('Usage: /set <seconds> <topic>')


def unset(update: Update, context: CallbackContext) -> None:
    """Remove the job if the user changed their mind."""
    chat_id = update.message.chat_id
    job_removed = remove_job_if_exists(str(chat_id), context)
    text = 'Timer successfully cancelled!' if job_removed else 'You have no active timer.'
    update.message.reply_text(text)


def echo(update: Update, context: CallbackContext) -> None:
    """Echo the user message."""
    update.message.reply_text(f"Undefined cmd: {update.message.text}")


def main() -> None:
    """Start the bot."""

    # Create the Updater and pass it your bot's token.
    updater = Updater(cfg["TOKEN"])

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # on different commands - answer in Telegram
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("set", set_timer))
    dispatcher.add_handler(CommandHandler("unset", unset))

    # on non command i.e message - echo the message on Telegram
    dispatcher.add_handler(
        MessageHandler(Filters.text & ~Filters.command, echo))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    with open("cfg.json") as f:
        cfg = json.load(f)
        run_file = cfg["run_file"]
        metrics_file = cfg["metrics_file"]
    main()
    # import subprocess
    # f = subprocess.Popen(['tail','-n 15',metrics_file],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    # metrics=f.stdout.readlines()
    
    # print()
  
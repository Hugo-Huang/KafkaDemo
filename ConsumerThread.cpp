#include "ConsumerThread.h"
#include <sys/time.h>
#include "AlarmRecallHandler.h"

using namespace std;

int RebalanceCb::eof_cnt = 0;
int RebalanceCb::partition_cnt = 0;
bool ConsumerThread::s_exitEOF = false;

static int count(const string& string, const string& sep)
{
    int cnt = 0;
    string::size_type index = string.find(sep, 0);
    while(index != string::npos) {
        ++cnt;
        index = string.find(sep, index + sep.size());
    }
    return cnt;
}

static vector<string>  split(const string& str, const string& sep)
{
    vector<string> result;
    result.reserve(count(str, sep) + 1);
    string::size_type start = 0;
    string::size_type index = str.find(sep, start);
    while(index != string::npos) {
        result.push_back(str.substr(start, index - start));
        start = index + sep.size();
        index = str.find(sep, start);
    }
    result.push_back(str.substr(start));
    return result;
}

ConsumerThread::ConsumerThread(int partition, const string &brokers, const string &topic)
{
    m_param.partition = partition;
    m_param.brokers = brokers;
    m_param.topic = topic;
    //linux platform
    m_process_id = 0;
}

ConsumerThread::~ConsumerThread()
{
    cout << "thread destruction:time-" << time(NULL) << endl;
    //linux platform
    if(m_process_id != 0)
    {
        pthread_cancel(m_process_id);
        //wait for deal last message
        pthread_join(m_process_id, NULL);
    }
    cout << "thread stoped normal:time-" << time(NULL) << endl;
}

bool ConsumerThread::MsgConsume(RdKafka::Message* message, void* opaque)
{
    bool ret = true;
    int	len = static_cast<int>(message->len());
    std::vector<char> data;
    string key;
    switch (message->err())
    {
    case RdKafka::ERR__TIMED_OUT:
        break;

    case RdKafka::ERR_NO_ERROR:
        //m_msgCount++;
        //m_msgBytes += message->len();
        if(message->key() == NULL)
        {
            cerr << "MsgConsume: Key is empty! " << endl;
            break;
        }
        key = *message->key();

        cout << "Key: " << key << endl;
        if(len > 0)
        {
            data.resize(len);
            memcpy(&data[0], message->payload(), len);

            if(key.compare("key_1") == 0)
            {
                cout << "Key1Handler HandleMsg" << endl;
            }
            else if(key.compare("key_2") == 0)
            {
                cout << "Key2Handler HandleMsg" << endl;
            }
            else if(key.compare("key_3") == 0)
            {
                cout << "Key3Handler HandleMsg" << endl;
            }
            else
            {
                cerr << "MsgConsume: message key not support: " << key << endl;
            }

            data.clear();
        }
        break;

    case RdKafka::ERR__PARTITION_EOF:
        if (s_exitEOF && ++RebalanceCb::eof_cnt == RebalanceCb::partition_cnt)
        {
            cerr << "%% EOF reached for all " << RebalanceCb::partition_cnt <<
                    " partition(s)" << endl;
            ret = false;
        }
        break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
        cerr << "Consume failed param: " << message->errstr() << endl;
        ret = false;
        break;

    default:
        /* Errors */
        cerr << "Consume failed error: " << message->errstr() << endl;
        ret = false;
        break;
    }
    return ret;
}

void ConsumerThread::Run()
{
    int retcode = pthread_create(&m_process_id, NULL, ConsumerThread::OpThread, (void*)&m_param);

    if (retcode != 0)
    {
        cout << "create thread failed" << endl;
        exit(0);
    }
    else
    {
        cout << "create thread OK" << endl;
    }
}

void* ConsumerThread::OpThread(void *param)
{
    KafkaParam* kp = (KafkaParam*)param;
    if(NULL == kp)
    {
        cerr << "OpThread: param is null" << endl;
        return;
    }

    string m_errstr;
    RebalanceCb ex_rebalance_cb;
    RdKafka::Conf *globalConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *topicConf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    globalConf->set("metadata.broker.list", kp->brokers, m_errstr);
    globalConf->set( "rebalance_cb", &ex_rebalance_cb, m_errstr);
    globalConf->set("group.id", "consume", m_errstr);
    globalConf->set("default_topic_conf", topicConf, m_errstr);
    delete topicConf;

    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(globalConf, m_errstr);
    if (!consumer)
    {
        cerr << "Failed to create consumer: " << m_errstr << endl;
        pthread_exit((void*)(long)1);
    }
    delete globalConf;

    cout << "% Created consumer " << consumer->name() << endl;
    vector<string> topics=split(kp->topic,",");
    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if (err)
    {
        cerr << "Failed to subscribe to " << topics.size() << " topics: "
             << RdKafka::err2str(err) << endl;
        pthread_exit((void*)(long)2);
    }
    //can cancel
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    int use_ccb=0;
    while(true)
    {
        if (use_ccb)
        {
            cerr << "Use callback: Not implemented" << endl;
            break;
        }

        RdKafka::Message *msg = consumer->consume(5000);
        if(msg != NULL)
        {
            bool ret = ConsumerThread::MsgConsume(msg, NULL);
            delete msg;
            if(!ret)
            {
                cerr << "thread consume error happened" << endl;
                break;
            }
        }
        pthread_testcancel();
    }
    pthread_exit((void*)(long)0);
}

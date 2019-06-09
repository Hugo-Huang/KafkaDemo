#include "rdkafka.h"
#include <string>
#include <unistd.h>
#include <sys/time.h>
#include <signal.h>
#include <syslog.h>
#include <errno.h>

using namespace std;

const int PRODUCER_INIT_FAILED = -1;
const int PRODUCER_INIT_SUCCESS = 0;
const int PUSH_DATA_FAILED = -1;
const int PUSH_DATA_SUCCESS = 0;

class ProducerKafka
{
public:
    ProducerKafka(){};
    ~ProducerKafka(){};

    void close();
    int initProducer(int partition, string ip, string topic);
    int sendMessage( char *bytes,int bytesSize, char *key, int keylen);

private:
    int partition_;

    //rd
    rd_kafka_t* handler_;
    rd_kafka_conf_t *conf_;

    //topic
    rd_kafka_topic_t *topic_;
    rd_kafka_topic_conf_t *topic_conf_;
};

static void logger(const rd_kafka_t *rk, int level,const char *fac, const char *buf)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
            (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
            level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

int ProducerKafka::initProducer(int partition, string ip,string topic)
{
    char tmp[16]={0};
    char errstr[512]={0};

    partition_ = partition;
    string brokers=ip;
    //brokers+=":9092";
    /* Kafka configuration */
    conf_ = rd_kafka_conf_new();

    //set logger :register log function
    rd_kafka_conf_set_log_cb(conf_, logger);

    /* Quick termination */
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf_, "internal.termination.signal", tmp, NULL, 0);
    //rd_kafka_conf_set(conf_,"message.max.bytes","10485760", NULL, 0);
    /*topic configuration*/
    topic_conf_ = rd_kafka_topic_conf_new();

    if (!(handler_  = rd_kafka_new(RD_KAFKA_PRODUCER, conf_, errstr, sizeof(errstr))))
    {
        fprintf(stderr, "*****Failed to create new producer: %s*******\n",errstr);
        return PRODUCER_INIT_FAILED;
    }

    rd_kafka_set_log_level(handler_, LOG_DEBUG);

    /* Add brokers */
    if (rd_kafka_brokers_add(handler_, brokers.c_str()) == 0)
    {
        fprintf(stderr, "****** No valid brokers specified********\n");
        return PRODUCER_INIT_FAILED;
    }


    /* Create topic */
    topic_ = rd_kafka_topic_new(handler_, topic.c_str(), topic_conf_);

    return PRODUCER_INIT_SUCCESS;
}

void ProducerKafka::close()
{
    /* Destroy topic */
    rd_kafka_topic_destroy(topic_);

    /* Destroy the handle */
    rd_kafka_destroy(handler_);
}

int ProducerKafka::sendMessage( char *buffer,int buf_len, char *key, int keylen)
{
    int ret;
    char errstr[512]={0};

    if(NULL == buffer)
        return -3;

    ret = rd_kafka_produce(topic_, partition_, RD_KAFKA_MSG_F_COPY,
                           (void*)buffer, (size_t)buf_len, (void*)key, keylen, NULL);

    if(ret == -1)
    {
        fprintf(stderr,"****Failed to produce to topic :bytes %d, %s partition %i: %s*****\n",
                buf_len,rd_kafka_topic_name(topic_), partition_,
                rd_kafka_err2str(rd_kafka_errno2err(errno)));

        rd_kafka_poll(handler_, 0);
        printf("MsgProducer Send Error!\n");
        return PUSH_DATA_FAILED;
    }

    fprintf(stderr, "***Sent %d bytes to topic:%s partition:%i*****\n",
            buf_len, rd_kafka_topic_name(topic_), partition_);

    rd_kafka_poll(handler_, 0);

    return PUSH_DATA_SUCCESS;
}

int main(int argc, char *argv[])
{
    ProducerKafka produce;
    if(PRODUCER_INIT_SUCCESS != produce.initProducer(0, "192.168.200.101", "alarm"))
    {
        return 1;
    }

    char buf[20] = {0};
    for(int i=0; i<3; i++)
    {
        sprintf(buf, "hello key %d", i+1);
        char key[10] = {0};
        sprintf(key, "key_%d", i+1);
        produce.sendMessage(buf, length, key, strlen(key));
        sleep(10);
    }

    return 0;
}

#ifndef CONSUMERTREAD_H
#define CONSUMERTREAD_H

#include <string>
#include <iostream>
#include "rdkafkacpp.h"

class RebalanceCb : public RdKafka::RebalanceCb {
private:
    static void part_list_print( const std::vector<RdKafka::TopicPartition*> &partitions )
    {
        for ( unsigned int i = 0; i < partitions.size(); i++ )
            std::cerr << partitions[i]->topic() <<
            "[" << partitions[i]->partition() << "], ";
        std::cerr << "\n";
    }


public:
    static int	eof_cnt;
    static int	partition_cnt;
    void rebalance_cb( RdKafka::KafkaConsumer *consumer,
               RdKafka::ErrorCode err,
               std::vector<RdKafka::TopicPartition*> &partitions )
    {
        std::cerr << "RebalanceCb: " << RdKafka::err2str( err ) << ": ";

        part_list_print( partitions );

        if ( err == RdKafka::ERR__ASSIGN_PARTITIONS )
        {
            consumer->assign(partitions);
            partition_cnt = partitions.size();
        } else {
            consumer->unassign();
            partition_cnt = 0;
        }
        eof_cnt = 0;
    }
};

class ConsumerThread
{
public:
    struct KafkaParam
    {
        int partition;
        std::string brokers;
        std::string topic;
    };

    ConsumerThread(int partition, const std::string &brokers, const std::string &topic);
    ~ConsumerThread();

    void Run();

    static void *OpThread(void *);
    static bool MsgConsume(RdKafka::Message* message, void* opaque);
    static bool s_exitEOF;

private:
    KafkaParam m_param;
    pthread_t m_process_id;
};

#endif // ConsumerThread_H

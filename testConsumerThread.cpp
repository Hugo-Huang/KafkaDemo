#include "ConsumerThread.h"
#include <unistd.h>

int main(int argc, char *argv[])
{
    AlarmConsumerThread thread(0, "192.168.200.101", "topic");
    thread.Run();
    int i = 120;
    while(i>0)
    {
        sleep(1);
        --i;
    }
}

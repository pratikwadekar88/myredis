#include<iostream>
#include<sys/socket.h>
#include<netinet/in.h>
#include<unistd.h>
#include<cstring>

int main(){
    int sockfd = socket(AF_INET,SOCK_STREAM,0);

    if(sockfd<0){
        std::cerr<<"Socket Failed\n";
        return 1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    int rv = bind(sockfd,(sockaddr*)&addr,sizeof(addr));

    if(rv<0){
        std::cerr<<" Bind Failed\n";
        return 1;
    }

    rv = listen(sockfd,5);
    if(rv<0){
        std::cerr<<" Listen Failed\n";
        return 1;
    }

    std::cout<<"Server Listening at Port 1234\n";
    while(1){
        int client_fd = accept(sockfd,nullptr,nullptr);

        if(client_fd<0){
            std::cerr<<"Accept Failed\n";
            return 1;
        }
        std::cout<<"Client Connected\n";

        char buf[1024];
        int n = recv(client_fd,buf, sizeof(buf),0);
        if(n<0){
            std::cerr<<"Recv Failed\n";
            return 1;
        }
        std::cout<<"Received: ";

        const char* response = "Hello From Myredis!\n";

        n = send(client_fd,response,strlen(response),0);

        if(n<0){
            std::cerr<<"Send Failed\n";
            return 1;
        }
        // Why .write -> cout << buf expects buf to be a null-terminated C string.
        std::cout.write(buf,n);
        std::cout<<"\n";
        close(client_fd);
    }
    close(sockfd);
    return 0;
}

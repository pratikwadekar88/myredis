#include<iostream>
#include<sys/socket.h>
#include<netinet/in.h>
#include<unistd.h>
#include<cstring>
#include<string>
#include<vector>
#include<unordered_map>

int find_crlf(const char* buf,int start, int n){
    for(int i = start;i+1<n;i++){
        if(buf[i]=='\r' && buf[i+1]=='\n'){
            return i;
        }
    }
    return -1;

};

std::vector<std::string> parse_request(const char*buf,int n){
    std::vector<std::string> args;
    if(n<4 || buf[0]!='*') return args;

    int pos = 0;
    int end = find_crlf(buf,pos,n);
    if(end==-1) return args;

    int argc = std::stoi(std::string(buf+1,end-1));

    pos = end+2;

    for(int i=0;i<argc;i++){
        if(pos>=n || buf[pos]!='$') return {};

        end = find_crlf(buf,pos,n);
        if(end==-1) return {};

        int len = std::stoi(std::string(buf + pos + 1, end - pos - 1));
        pos = end+2;
        if(pos+len+2>n) return {};

        args.emplace_back(buf + pos, len);

        pos += len;

        if (pos + 1 >= n || buf[pos] != '\r' || buf[pos + 1] != '\n') {
            return {};
        }

        pos += 2;
    }
    return args;
}

std:: string handle_command(const std::vector<std::string> &args,std::unordered_map<std::string,std::string> &store){
    if(args.empty()){
        return "-ERR Empty command\r\n";
    }
    if(args[0]=="PING"){
        return "+PING\r\n";
    }
    if(args[0]=="SET"){
        if(args.size()!=3){
            return "-ERR wrong number of arguments\r\n";
        }
        store[args[1]] = args[2];
        return "+OK\r\n";
    }
    if(args[0]=="GET"){
        if(args.size()!=2){
            return "-ERR Wrong number of arguments\r\n";
        }
        auto it = store.find(args[1]);
        if(it==store.end()) return "$-1\r\n";
        return "$" + std::to_string(it->second.size()) + "\r\n" + it->second + "\r\n";
    }
    return "-ERR Unknown command\r\n";
}


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
    int client_fd = -1;
    std::cout<<"Server Listening at Port 1234\n";
    std::unordered_map<std::string,std::string> store;
    while(1){
        client_fd = accept(sockfd,nullptr,nullptr);

        if(client_fd<0){
            std::cerr<<"Accept Failed\n";
            return 1;
        }
        std::cout<<"Client Connected\n";
        while(1){ 
            char buf[4096];
            int n = recv(client_fd,buf, sizeof(buf),0);
            if(n<=0){
                std::cerr<<"Recv Failed\n";
                close(client_fd);
                break;
            }
            //std::cout.write(buf,n);
            std::cout<<"Received "<<n<<" bytes\n";
            //for(int i=0;i<n;i++){
             //   std::cout<<(int)(unsigned char)buf[i]<<" ";
           // }
            // if(buf[0]!='*'){
            //     std::cerr<<"Invalid req\n";
            //     continue;
            // }
            // int argc = buf[1]-'0';
            // std::cout<<"Arguments: "<<argc<<"\n";

            // int pos = 0;
            // int end = find_crlf(buf,pos,n);
            // int len = std::stoi(std::string(buf+1,end-1));
            // pos = end+2;

            // std::string command(buf+pos,len);

            // std::cout<<"Command: "<<command<<"\n";
            // std::cout<<"\n";

            std::vector<std::string> args = parse_request(buf,n);
            if(args.empty()){
                std::cerr<<"Invalid Request\n";
                continue;
            }
            std::cout<<"Command: "<<args[0]<<"\n";
            for(const auto& arg:args){
                std::cout<<arg<<"\n";
            }

            // const char* response = "+PONG\r\n";
            std::string response = handle_command(args,store);

            n = send(client_fd,response.data(),response.size(),0);

            if(n<0){
                std::cerr<<"Send Failed\n";
                return 1;
            }
        }
        close(client_fd);
        // Why .write -> cout << buf expects buf to be a null-terminated C string.
        std::cout<<"\n";
    }
    close(sockfd);
    return 0;
}

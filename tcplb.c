/*
	Author: Tornike Abuladze
	Assign: TCP traffic load balancer
	Lector: George Kobiashvili
*/
 #include <netdb.h>
 #include <ifaddrs.h>
 #include <sys/types.h>  
 #include <sys/socket.h> 
 #include <netinet/in.h> 
 #include <arpa/inet.h>  
 #include <stdio.h>       
 #include <stdlib.h>      
 #include <string.h>      
 #include <unistd.h>   
 #include <inttypes.h>
 #include "uthash.h"
 #include <pthread.h> 
 #include <semaphore.h>
 #include <err.h>
 #include <string.h>
 #include <stdio.h>

 #define BUFF_SIZE 512
 #define SOCKET_BUFF_SIZE 4096

 
 #define _GNU_SOURCE
 #include <string.h>	
 char *strcasestr(const char *haystack, const char *needle);

//მაპის სტრუქტურა,რათა შევინახოთ სერვერის სახელზე სერვერის,მისმართი
//სერვერს სახელს მე თვითონ ვანიჭებ ტსპ ს შემთხვევაში
struct server_names_struct {
    char name[BUFF_SIZE];             /* key (string is WITHIN the structure) */
    char* ip;
    UT_hash_handle hh;         /* makes this structure hashable */
};
struct server_names_struct * servers=NULL;
//მაპის სტრუქტურა, რათა შევინახოთ უსერის ip:port წყვილზე სერვერის სახელი, რათა იგივე ip და იგივე 
//პორტიდან მომართვის შემთხვევაში გავუშვათ იგივე სერვერზე;
struct user_ip_port_to_server_struct{
	char name[BUFF_SIZE];
	char* server_name;
	UT_hash_handle hh;
};
struct user_ip_port_to_server_struct *users;

//ამ სტრუქტურაში მოთავსებული ინფორმაცია საერთოა ყველასთვის;
struct shared_info{
	int number_of_servers;
	int curr_server_index;
	sem_t servers_array_semaphore;
	char** servers_array;
	int* domains_array;
	sem_t* domains_semaphore;
};
//სრედებისთვის აუცილებელი ინფორმაცია
struct th_info{
	int user_fd;
	char* port;
	struct shared_info* shared;
};

//tcp შეერთების სრედი 
void* tcp_connection_handler(void* info)
{
	int user_fd=((struct th_info*)info)->user_fd;
	char* port= ((struct th_info*)info)->port;
	struct shared_info * shared=((struct th_info*)info)->shared;

	//საიდან მომივიდა რომელი იპ და რომელი პორტიდან
	struct sockaddr_in from_addr;
    uint32_t from_len;
    from_len = sizeof(from_addr);
    
    getpeername(user_fd,(struct sockaddr *)&from_addr,&from_len);
    char *from_addr_ip=inet_ntoa(from_addr.sin_addr);
    uint16_t from_addr_port=ntohs(from_addr.sin_port);
    //აქ გავიგე კლირნტი რომელი ip და რომელი port ით დამიკავშირდა
    //აქვე ავაწყობ მაპის key-ს, ip:port და ასე მოვძებნი map_ში. 
    char *ip_port_key=malloc(BUFF_SIZE);
    sprintf(ip_port_key,"%s:%u",from_addr_ip,from_addr_port);
	int back_end_server_fd;
	struct sockaddr_in back_end_server_addr;
	struct in_addr back_end_server_ip_addr;

	back_end_server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (back_end_server_fd==-1)
		err(1,"create socket");

	struct user_ip_port_to_server_struct* ss;
	HASH_FIND_STR(users,ip_port_key, ss);
	int count=0;

	char* server_name;
	struct server_names_struct *s;
	char*ip;
	socklen_t addrlen;
	int all_server_died=0;

	int ip_port_key_is_in_map=0;
	printf("%s\n",ip_port_key);
	//ვცდი დაკავშირებას 
	if(ss!=NULL)
	{
		server_name=ss->server_name;
		HASH_FIND_STR(servers,server_name, s);
		ip=s->ip;
		inet_pton(AF_INET, ip, &back_end_server_ip_addr);
		back_end_server_addr.sin_family = AF_INET;
		back_end_server_addr.sin_port = htons(atoi(port));
		back_end_server_addr.sin_addr = back_end_server_ip_addr;
		addrlen = sizeof(struct sockaddr_in);
		ip_port_key_is_in_map=1;
	}
	//თუ ვერ დავუკავშირდით ზემოთ ცდით სერვერს, ან თუ იპ:პორტი უნიკალურია 
	//მაშინ რომელიმე მიმდინარე სერვერზე გავუშვებთ, ამ სერვერებზე ვცდით სერვერების რაოდენობაჯერ
	//თუ ვერცერთხელ მოხდა დაკავსირება ე.ი ყველა სერვერი მკვდარია და მოთხოვნას ვუგულებელყოფ
	//რადგან ამ დროს კიდე ბევრი რომ შემოვიდეს იუზერი ყველა რომ ლოდინის რეჟიმში იყოს ცუდია
	if(ss==NULL || connect(back_end_server_fd,(const struct sockaddr *) &back_end_server_addr, addrlen)!=0)
	{
		while(count<shared->number_of_servers)
		{
			count++;
			//ამ ნაწილს ვლოქავ იმიტომ რომ რომ არ დავლოქო შეძლება მიმდინარე სერვერის
			//ინდექსს რომ ვზრდი სანამ დაინაშთება სხვა სრედმა წამოიღოს ეს ინდექსი
			//და საზღვარს გასცდეს ამიტომ დავლოქავ და ატომური გახდება მიმატება და შემდეგ დანაშთვა
			sem_wait(&(shared->servers_array_semaphore));
		
			server_name = shared->servers_array[shared->curr_server_index];
			shared->curr_server_index=(shared->curr_server_index+1)%shared->number_of_servers;
		
			sem_post(&(shared->servers_array_semaphore));
		
			HASH_FIND_STR(servers,server_name, s);
			char*ip=s->ip;
			inet_pton(AF_INET, ip, &back_end_server_ip_addr);

			back_end_server_addr.sin_family = AF_INET;
			back_end_server_addr.sin_port = htons(atoi(port));
			back_end_server_addr.sin_addr = back_end_server_ip_addr;

			addrlen = sizeof(struct sockaddr_in);

			if(connect(back_end_server_fd,(const struct sockaddr *) &back_end_server_addr, addrlen)==0)
			{
				if(ip_port_key_is_in_map==1)
					HASH_DEL(users, ss);
				struct user_ip_port_to_server_struct *new_entry=
						(struct user_ip_port_to_server_struct*)malloc(sizeof(struct user_ip_port_to_server_struct));

				strncpy(new_entry->name,ip_port_key,BUFF_SIZE);
				new_entry->server_name=server_name;
				HASH_ADD_STR(users,name,new_entry);
				break;
			}
		}
		if(count>shared->number_of_servers)
			all_server_died=1;
	}
	// აქ რომ მოვედი თუ რომელიმე სერვერს დავუკავშირდდი
	//მაშინ უნდა გავაკეთო წაკითხვა accept ით მიღებული სოკეტიდან 
	//გადავიტანოთ ბუფერში და ვუგზავნოთ სერვერს რომელსაც დავუქონექთდით
	//შემდეგ დაველოდოთ პასუხს იგივე სოკეტზე და შემდეგ მივიღოთ ბუფერში
	//და ისევ გავუგზავნოთ კლიენტს
	printf("TCP %s > %s:%s",ip_port_key,ip,port);
	if(all_server_died==0)
	{
		while(1)
		{
			char buff[SOCKET_BUFF_SIZE];
			int concrete_packet_size=read(user_fd,buff,SOCKET_BUFF_SIZE);
			if(concrete_packet_size<=0)
				break;
			write(back_end_server_fd,buff,concrete_packet_size);
		}
		while(1)
		{
			char buff[SOCKET_BUFF_SIZE];
			int concrete_packet_size=read(back_end_server_fd,buff,SOCKET_BUFF_SIZE);
			if(concrete_packet_size<=0)
				break;
			write(user_fd,buff,concrete_packet_size);	
		}
	}
	close(user_fd);
	close(back_end_server_fd);
}
//აბალანსებ http შეერთებებს სერვერებზე
void* http_connection_handler(void*info)
{
	int user_fd=((struct th_info*)info)->user_fd;
	char* port= ((struct th_info*)info)->port;
	struct shared_info * shared=((struct th_info*)info)->shared;

	//საიდან მომივიდა რომელი იპ და რომელი პორტიდან
	struct sockaddr_in from_addr;
    uint32_t from_len;
    from_len = sizeof(from_addr);
    
    getpeername(user_fd,(struct sockaddr *)&from_addr,&from_len);
    char *from_addr_ip=inet_ntoa(from_addr.sin_addr);
    uint16_t from_addr_port=ntohs(from_addr.sin_port);
    //აქ გავიგე კლირნტი რომელი ip და რომელი port ით დამიკავშირდა
    //ip:port. 
    char *ip_port_key=malloc(BUFF_SIZE);
    sprintf(ip_port_key,"%s:%u",from_addr_ip,from_addr_port);

	int back_end_server_fd;
	struct sockaddr_in back_end_server_addr;
	struct in_addr back_end_server_ip_addr;

	back_end_server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (back_end_server_fd==-1)
		err(1,"create socket");

	char* server_name;
	struct server_names_struct *s;
	char*ip;
	socklen_t addrlen;
	int all_server_died=0;


	//ახლა წამოვიღებთ გარკვეული ზომის ინფორმაციას სოკეტიდან
	//ბუფერი მაქ აღებული იმხელა რომ აუცილებლად ამ პორციაში მოხვდება 
	//ჰედერი ამიტომ წამოვიღებ 1 პორციას და მასში ვნახავ coocie ში თუ რამე მაქვს;

	char contains_header[SOCKET_BUFF_SIZE];
	int header_size=read(user_fd,contains_header,SOCKET_BUFF_SIZE);
	
	char* host_p=strcasestr(contains_header,"Host");
	char* line_h=strtok(host_p,"\r");
    char* host_print=strcasestr(line_h," ");
    host_print++;

    int lef=-1;
    int rig=0;
    int ll;
    int idex_in_domains=0;
    //სერვერები მაქ ყველა ერთ მასივში და ახლა დომეინზე ვეძებ ამ დომეინის სერვერები
    //მასივის რა დიაპაზონზეა რომ იმაზე ჩავციკლო შემდეგ
    for(ll=0; ll<shared->number_of_servers; ll++)
    	if(strstr(shared->servers_array[ll],host_print)!=NULL)
    		if(lef>-1)
    			rig++;
    		else
    		{
    			lef=ll;
    			rig=ll;
    		}
    if(lef==-1)
    {
    	close(user_fd);
    	return;
    }
    for(ll=1; ll<=lef; ll++)
    {
    	char* a=malloc(BUFF_SIZE);
    	strcpy(a,shared->servers_array[ll-1]);
    	char* b=malloc(BUFF_SIZE);
    	strcpy(b,shared->servers_array[ll]);
    	
    	char* fir=strtok(a,":");
    	char* sec=strtok(b,":");
    	if(strcmp(fir,sec)!=0)
    		idex_in_domains++;
    	free(a);
    	free(b);
    }

    int mod=rig-lef+1;

	char * cookie_pointer=strcasestr(contains_header,"Cookie:");
	if(cookie_pointer!=NULL)
	{
		char * cookie_line=strtok(cookie_pointer,"\r");
		char * serever_pointer=strcasestr(cookie_line,"tcplb");
		char * serever_pointer_end=strtok(serever_pointer," ");
		char * server_value=strcasestr(serever_pointer_end,"=");
		server_value++;
		server_name=server_value;

		HASH_FIND_STR(servers,server_name, s);
		ip=s->ip;
		inet_pton(AF_INET, ip, &back_end_server_ip_addr);
		back_end_server_addr.sin_family = AF_INET;
		back_end_server_addr.sin_port = htons(atoi(port));
		back_end_server_addr.sin_addr = back_end_server_ip_addr;
		addrlen = sizeof(struct sockaddr_in);
	}
	int coun=0;
	int curr_server=0;

	if(cookie_pointer==NULL ||
		connect(back_end_server_fd,(const struct sockaddr *) &back_end_server_addr,addrlen)!=0)
	{
		while(coun<mod)
		{
			coun++;
			sem_wait(&(shared->domains_semaphore[idex_in_domains]));
			printf("lockis %d\n", shared->domains_array[idex_in_domains]);
			server_name = shared->servers_array[lef+shared->domains_array[idex_in_domains]];
			shared->domains_array[idex_in_domains]=(shared->domains_array[idex_in_domains]+1)%mod;

			sem_post(&(shared->domains_semaphore[idex_in_domains]));
	
			HASH_FIND_STR(servers,server_name, s);
			ip=s->ip;
			inet_pton(AF_INET, ip, &back_end_server_ip_addr);

			back_end_server_addr.sin_family = AF_INET;
			back_end_server_addr.sin_port = htons(atoi(port));
			back_end_server_addr.sin_addr = back_end_server_ip_addr;

			addrlen = sizeof(struct sockaddr_in);
			if(connect(back_end_server_fd,(const struct sockaddr *) &back_end_server_addr, addrlen)==0)
			{
				break;
			}
		}
		if(coun>mod)
			all_server_died=1;
	}
	if(all_server_died==0)
	{
		printf("TCP %s > %s:%s cookie=%s\n",ip_port_key,ip,port,server_name);
		write(back_end_server_fd,contains_header,header_size);
		while(1)
		{
			char buff[SOCKET_BUFF_SIZE];
			int concrete_packet_size=read(user_fd,buff,SOCKET_BUFF_SIZE);
			if(concrete_packet_size<=0)
				break;
			write(back_end_server_fd,buff,concrete_packet_size);
		}
		char respnse_header_buff[SOCKET_BUFF_SIZE];
		char updated_header_buff[2*SOCKET_BUFF_SIZE];
		//ამოვიღოთ ვთვლით რომ ჰედერი ამ ზომაში ეტევა
		int resp_header_size=read(back_end_server_fd,respnse_header_buff,SOCKET_BUFF_SIZE);

		char* point=strcasestr(respnse_header_buff,"\n");
		point++;
		
		point=strcasestr(point,"\n");
		point++;

		point=strcasestr(point,"\n");
		point++;

		int len=(int)(point-respnse_header_buff); //სათავიდან რამდენი ბაიტითაა დაშორებული

		strncat(updated_header_buff,respnse_header_buff,len);

		char * add=malloc(BUFF_SIZE);

		sprintf(add,"Set-cookie: tcplb=%s",server_name);

		int aa=strlen(add);

		strcat(updated_header_buff,add);
		strcat(updated_header_buff,"\n");

		strcat(updated_header_buff,point); //ჩავაკვეხეთ

		resp_header_size=resp_header_size+aa;

		//განვაახლე ჰედერი და ახლა გავუშვებ
		write(user_fd,updated_header_buff,resp_header_size);
		while(1)
		{
			char buff[SOCKET_BUFF_SIZE];
			int concrete_packet_size=read(back_end_server_fd,buff,SOCKET_BUFF_SIZE);
			if(concrete_packet_size<=0)
				break;
			write(user_fd,buff,concrete_packet_size);	
		}
	}
	close(user_fd);
	close(back_end_server_fd);
}
//ეს მეთოდი უზრუნველყოფს შეერთებების დაბალანსებას
//აქ ხდება გაყოფა tcp და http შეერთებებს შორის.
void balance(struct shared_info* shared,char *port,char* traffic_type)
{
	struct sockaddr_in svr_addr;
	int server_socket;
	//სოკეტის გახსნა
	server_socket=socket(AF_INET,SOCK_STREAM, 0);
	if(server_socket<0)
		err(1,"cannot open socket");
	svr_addr.sin_family = AF_INET;
  	svr_addr.sin_addr.s_addr = INADDR_ANY;
  	svr_addr.sin_port = htons(atoi(port));

  	//დაბაინდება სოკეტის
  	if (bind(server_socket,(struct sockaddr *) &svr_addr, sizeof(svr_addr)) == -1) {
    	close(server_socket);
    	err(1,"Can't bind");
  	}

  	if(listen(server_socket,SOMAXCONN)==-1)
  		err(1,"listen");
  	while(1)
  	{
  		//მომხმარებელი რომელიც დამიკავშირდება
  		int user_fd;
  		struct sockaddr_in user_addr;
  		socklen_t sin_len = sizeof(user_addr);

  		pthread_t con_thread;
  		user_fd=accept(server_socket, (struct sockaddr *)&user_addr, &sin_len);
   			if (user_fd == -1)
      					continue;
      	struct th_info  *info=(struct th_info  *)malloc(sizeof(struct th_info));
      	info->user_fd=user_fd;
      	info->shared=shared;
      	info->port=port;
      	if(strcmp(traffic_type,"tcp")==0)
      		pthread_create(&con_thread, NULL ,tcp_connection_handler,(void*)info);
      	else
      		pthread_create(&con_thread, NULL ,http_connection_handler,(void*)info);
  	}
}
int main(int argc, char const *argv[])
{
	/* code */
	struct shared_info* shared=malloc(sizeof(struct shared_info));
	sem_init(&(shared->servers_array_semaphore),0,1);

	char * traffic_type=(char*)argv[1];	
	char * port;
	if(strcmp(traffic_type,"tcp")==0)
	{
		port=(char*)argv[2];	
		int i;
		char* server_name=(char*)malloc(BUFF_SIZE);
		strcpy(server_name,"server");
		struct server_names_struct * s,*tmp;
		
		//shared infos შევსება
		shared->servers_array=malloc((argc-2)*sizeof(char*));
		shared->number_of_servers=argc-3;
		shared->curr_server_index=0;

		for(i = 3; i<argc; i++)
		{
			char * concrete_server_name=(char*)malloc(BUFF_SIZE);
			sprintf(concrete_server_name,"%s%d",server_name,i-2);
			s=(struct server_names_struct*)malloc(sizeof(struct server_names_struct));
			strncpy(s->name,concrete_server_name,BUFF_SIZE);
			s->ip=(char*)argv[i];
			shared->servers_array[i-3]=concrete_server_name;
			HASH_ADD_STR(servers,name, s );
		}
    	balance(shared,port,traffic_type);
	}
	//თუ http შეერთებების ბალანსირება მიწევს
	if(strcmp(traffic_type,"http")==0)
	{
		port=malloc(BUFF_SIZE);
		strcpy(port,"80");

		int i;
		for(i =2; i<argc; i++)
		{
			char * domain=strtok((char*)argv[i],"=");
			char * ips=strcasestr((char*)argv[i],"=");
			char * server_name=(char*)malloc(BUFF_SIZE);
			sprintf(server_name,"%s%s",domain,"-server:");
			char* concrete_ip=strtok(ips,",");
			int index=0;
			struct server_names_struct * s;
			while(concrete_ip!= NULL ) 
			{
				index++;
				char* concrete_server_name=malloc(BUFF_SIZE);
				sprintf(concrete_server_name,"%s%d",server_name,index);
				//printf("%s\n",concrete_server_name);

				s=(struct server_names_struct*)malloc(sizeof(struct server_names_struct));
				strncpy(s->name,concrete_server_name,BUFF_SIZE);
				s->ip=concrete_ip;
				HASH_ADD_STR(servers,name,s);

				concrete_ip = strtok(NULL,",");
			}
		}
		int number_of_servers=0;
		struct server_names_struct * s,*tmp;
		HASH_ITER(hh, servers, s,tmp)
			number_of_servers++;
		shared->servers_array=malloc(number_of_servers*sizeof(char*));
		shared->number_of_servers=number_of_servers;
		shared->curr_server_index=0;
		shared->domains_array=malloc((argc-2)*sizeof(int));
		int pp;
		for(pp =0; pp<argc-2; pp++)
			shared->domains_array[pp]=0;
		shared->domains_semaphore=malloc((argc-2)*sizeof(sem_t));
		for(pp =0; pp<argc-2; pp++)
			sem_init(&(shared->domains_semaphore[pp]),0,1);

		int ind=0;
		HASH_ITER(hh, servers, s,tmp)
		{
			shared->servers_array[ind]=s->name;
			ind++;
		}
		balance(shared,port,traffic_type);
    }
	pthread_exit(NULL);
	return 0;
}
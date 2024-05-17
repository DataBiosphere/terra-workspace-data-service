#set wdl version
version 1.0

#add and name a workflow block
workflow hello_world {
   call hello
   output { File helloFile = hello.outFile }
}
#define the 'hello' task
task hello {
    input {
    File myName
    }
    #define command to execute when this task runs
    command {
        echo Hello World! > Hello.txt
        cat ${myName} >> Hello.txt
    }
    #specify the output(s) of this task so cromwell will keep track of them
    output {
        File outFile = "Hello.txt"
    }

    #specify runtime attributes
    runtime {
        docker: "ubuntu:latest"
        cpu: 8
        memory: "7 GB"
        disk: "10 GB"
        maxRetries: 1
        timeout: "1 minute"
    }
}
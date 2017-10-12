Massive Data

Vm步骤

- ssh -I ….+你 instance的信息
- 然后就连上了。




Vm:
	$ ssh -i ~/.ssh/id_rsa.pem ec2地址


EMR
	$ ssh -a hadoop@ec2-###-##-##-###.compute-1.amazonaws.com -i ~/mykeypair.pem
Replace ec2-###-##-##-###.compute-1.amazonaws.com with the master public DNS name of your cluster and replace

参考网址：http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html



是在configure 新的 vm的时候用了iam 里的 access key。create新key的时候下载了csv文件。


Git
sudo yum install -y git

run.py
python34 run.py --input s3://gu-anly502/A1/quazyilx2.txt

A2

跑python的时候，我猜的给参数的格式是：
 Python run.py - - output xxx(xxx为参数)



How to copy file from S3 to HDFS
http://stackoverflow.com/questions/7487292/how-do-i-copy-files-from-s3-to-amazon-emr-hdfs
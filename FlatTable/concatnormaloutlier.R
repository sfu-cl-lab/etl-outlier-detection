mainDirecotry<-"/local-scratch/SRiahi/workSpaceJan/FlatTable/realworld-wordification-march1"

F<-c("Premier_League_MidStr","Premier_League_Strikers","imdb_MovieLens_Drama");
V<-c("Bigrams","IDF","Unigrams")

for(i in V){
for(j in F){
a<-paste(mainDirecotry,"/CSVOutlier/",i,"/",j,".csv",sep="")
b<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,".csv",sep="")


ts1<-read.csv(file=a, head=FALSE, sep=",")
ts2<-read.csv(file=b, head=FALSE, sep=",")
ss<-rbind(ts1,ts2)
temp<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,"Combined.csv",sep="")
write.table(ss, file=temp, sep=",", quote=FALSE, row.names=FALSE, col.names=FALSE)
}



}

#mainDirecotry<-"/local-scratch/SRiahi/workSpaceJan/FlatTable"

/local-scratch/SRiahi/workSpaceJan/FlatTable/Wordification-Realworld/Bigram
mainDirecotry<-"/local-scratch/SRiahi/workSpaceJan/FlatTable/Wordification-Synthetic"
F<-c("Premier_League_Synthetic_Bernoulli_Feature","Premier_League_Synthetic_Bernoulli_Sep","Premier_League_Synthetic_Bernoulli_SV");
V<-c("Bigrams","IDF","Unigrams")

for(i in V){
for(j in F){
a<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,".csv",sep="")
b<-paste(mainDirecotry,"/CSVOutlier/",i,"/",j,".csv",sep="")


file.remove(a)
file.remove(b)
temp<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,"Combined.csv",sep="")
file.remove(temp)
}



}




mainDirecotry<-"/local-scratch/SRiahi/workSpaceJan/FlatTable"

F<-c("Premier_League_Synthetic_Bernoulli_Feature","Premier_League_Synthetic_Bernoulli_Sep","Premier_League_Synthetic_Bernoulli_SV");
V<-c("IDF")

for(i in V){
for(j in F){
a<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,".csv",sep="")
b<-paste(mainDirecotry,"/CSVOutlier/",i,"/",j,".csv",sep="")


ts1<-read.csv(file=a, head=FALSE, sep=",")
ts2<-read.csv(file=b, head=FALSE, sep=",")
ss<-rbind(ts1,ts2)
temp<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,"Combined.csv",sep="")
write.table(ss, file=temp, sep=",", quote=FALSE, row.names=FALSE, col.names=FALSE)
}

}





sourceDirectory <-"/local-scratch/SRiahi/elki/LOF-Synthetic-IDF"
dirnames<-list.dirs(sourceDirectory,full.names=TRUE )
for(dir in dirnames){
if(!grepl(dir, "/local-scratch/SRiahi/elki/LOF-Synthetic-IDF")&&!grepl(dir, "/local-scratch/SRiahi/ELKI/ElkiData")){
filenames<-list.files(dir,pattern="Result.txt", full.names=TRUE )
ts1<-read.csv(file=filenames, head=FALSE, sep=" ")
#ts1<-read.csv(file=temp, head=FALSE, sep=" ")
		ts1$V1<-gsub("1", "2",ts1$V1)

		ts1$V1<-gsub("0", "3",ts1$V1)
#ts1<-read.csv(file="/local-scratch/SRiahi/elki/LOF-Synthtic-Bigrams/LOF-SV-Bigrams/test.txt", head=FALSE, sep="")
#pred1<-prediction(ts1$V1, ts1$V2)

	
filename=c(dir,"/Result2.txt")
t<-paste(filename, collapse="")
file.remove(t)
#z<-unlist(ts1$V4)
#z2<-unlist(ts1$V5)
x<-data.frame(ts1$V1,ts1$V2)
write.table(x, file=t,row.names=FALSE, col.names=FALSE, quote=FALSE)
}

}



############################################################3
sourceDirectory <-"/local-scratch/SRiahi/elki/March2ndExperiments-RealworldData/lof-wordification"
first<-0;
counter<-0;
v2<-rainbow(length(dirnames))
dirnames<-list.dirs(sourceDirectory,full.names=TRUE )
for(dir in dirnames){
counter<-counter+1;
if(!grepl(dir, sourceDirectory)&&!grepl(dir, "/local-scratch/SRiahi/ELKI/ElkiData")){
filenames<-list.files(dir,pattern="_order.txt", full.names=TRUE )
ts1<-read.csv(file=filenames, head=FALSE, sep=" ")
size<-length(ts1)

		ts1[,size-1]<-gsub("Outlier", "2",ts1[,size-1])

		ts1[,size-1]<-gsub("Normal", "1",ts1[,size-1])

	ts1[,size]<-gsub(".*=", "", ts1[,size])
	
filename=c(dir,"/Result.txt")
t<-paste(filename, collapse="")
file.remove(t)
#z<-unlist(ts1$V4)
#z2<-unlist(ts1$V5)
x<-data.frame(ts1[,size-1],ts1[,size])
write.table(x, file=t,row.names=FALSE, col.names=FALSE, quote=FALSE)
}
first<-1
}




#sourceDirectory <-"/local-scratch/SRiahi/elki/Feb23Experiments/OurMethod"
dirnames<-list.dirs(sourceDirectory,full.names=TRUE )
for(dir in dirnames){
if(!grepl(dir, sourceDirectory)&&!grepl(dir, "/local-scratch/SRiahi/ELKI/ElkiData")){
temp<-paste(dir,"/Result.txt",sep="")
ts1<-read.csv(file=temp, head=FALSE, sep=" ")
#ts1<-read.csv(file="/local-scratch/SRiahi/elki/LOF-Synthtic-Bigrams/LOF-SV-Bigrams/test.txt", head=FALSE, sep="")
pred1<-prediction(ts1$V2, ts1$V1)
perf1 <- performance( pred1, "tpr", "fpr" )
a<-performance(pred1,"auc")
	
filename=c(dir,"/ResultAUC.txt")
t<-paste(filename, collapse="")
file.remove(t)
#z<-unlist(ts1$V4)
#z2<-unlist(ts1$V5)

dput(a, file=t)
}

}

###########################33



mainDirecotry<-"/local-scratch/SRiahi/workSpaceJan/FlatTable/Wordification-Realworld/Bigram"
F<-c("Midfielder","Strikers");
V<-c("Bigrams","IDF")

for(i in V){
for(j in F){
a<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,".csv",sep="")
b<-paste(mainDirecotry,"/CSVOutlier/",i,"/",j,".csv",sep="")


file.remove(a)
file.remove(b)
temp<-paste(mainDirecotry,"/CSVNormal/",i,"/",j,"Combined.csv",sep="")
file.remove(temp)
}



}



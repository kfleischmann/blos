#USAGE
#
#Source provided by
# http://www.geolink.pt/ecmlpkdd2015-challenge/dataset.html
#
#FUNCTION PARAMETERS: @filename
#@submission: path+filename containing trajectories in CSV format
process.filename<-function(filename)
{
	libraries()
	dt<-read.csv2(filename,sep=",")

	dt$CALL_TYPE<-as.character(dt$CALL_TYPE)
	dt$DAY_TYPE<-as.character(dt$DAY_TYPE)
	dt$MISSING_DATA<-as.character(dt$MISSING_DATA)
	dt$POLYLINE<-as.character(dt$POLYLINE)
	str(dt)
	print(unique(dt$MISSING_DATA))

	my_poly<-dt$POLYLINE[2]
	print(my_poly)
	print("")
	i<-1

	for (mp in c(1:length(dt$POLYLINE)))
	{
		print(mp)
		print(miliseconds_to_date(dt$TIMESTAMP[mp]))
		my_poly<-dt$POLYLINE[mp]
		if(nchar(my_poly)>2)
		{
			ab<-unlist(strsplit(my_poly,"[]]"))
			polyline<-c(substr(ab[1],3,nchar(ab[1])),sapply(ab[2:(length(ab)-1)],my.substr))

			lat<-c(as.numeric(sapply(polyline,getlat)))
			lon<-c(as.numeric(sapply(polyline,getlon)))

			create_map(length(dt$POLYLINE),lat,lon)
		}
		print(mp)
		print(miliseconds_to_date(dt$TIMESTAMP[mp]))
		x<-scan()
	}
}

# expect data frame
# c(lat, lon)
process.data <- function(filename)
{

	center3 <- c(lat=41.168012,lon=-8.688717)
	center2 <- c(lat=41.155231,lon=-8.672868)
	center1 <- "Porto"

	center <- c(41.1492123,-8.5877372)	# hbf

	maxshort_tripsize <- 10
	zoom <- 12
	resolution <- 3
	normalize <- function(x) { round(x, digits=resolution)}
	libraries()
	dt<-read.csv2(filename,sep="\t")
	colfunc<-colorRampPalette(c("red","red"))

	dt_long <- subset(dt, duration >= maxshort_tripsize )
	dt_short <- subset(dt, duration < maxshort_tripsize )

	point_size <- 1

	latLong <- sapply( as.numeric( as.vector( dt_long$lat )), normalize)
	lonLong <- sapply( as.numeric( as.vector( dt_long$lon )), normalize)

	latShort <- sapply( as.numeric( as.vector( dt_short$lat )), normalize)
	lonShort <- sapply( as.numeric( as.vector( dt_short$lon )), normalize)


	png(paste(filename,"_short.png", sep=""),type='cairo-png')
	MyMapShort <- GetMap(center=center, zoom=zoom,GRAYSCALE=FALSE,destfile = paste(filename,"_short.png", sep=""));
	tmp <- PlotOnStaticMap (MyMapShort, lat = latShort, lonShort, cex=point_size,pch=20, add=FALSE,  col=c("green"), alpa=0.5 )
	dev.off()

	png(paste(filename,"_long.png", sep=""),type='cairo-png')
	MyMapLong <- GetMap(center=center, zoom=zoom,GRAYSCALE=FALSE,destfile = paste(filename,"_long.png", sep=""));
	tmp <- PlotOnStaticMap (MyMapLong, lat = latLong, lonLong, cex=point_size,pch=20, add=FALSE, col=c("red"), alpa=0.5 )
	dev.off()
}

miliseconds_to_date<-function(dt)
{
	mdy<-month.day.year((dt/3600/24))

	dt<-dt%%(3600*24)
	h<-floor(dt/3600)
	dt<-dt-(h*3600)
	m<-floor(dt/60)
	dt<-dt-(m*60)
	s<-round(dt)

	final<-sprintf("%s/%s/%s %s:%s:%s",format_alg(4,mdy$year),format_alg(2,mdy$month),format_alg(2,mdy$day),format_alg(2,h),format_alg(2,m),format_alg(2,s))
	return(final)
}

create_map<-function(ntraj,lat,lon,zoom=13 )
{

	#random colors
	colfunc<-colorRampPalette(c("green","red"))
	center = c(mean(lat), mean(lon))

	print(sprintf("Zoom: %d",zoom))
	MyMap <- GetMap(center="Porto", zoom=zoom,GRAYSCALE=FALSE,destfile = "MyTile3.png");

	point_size<-1.0

	print(lat)
	print(lon)

	tmp <- PlotOnStaticMap (MyMap, lat = lat,lon = lon, cex=point_size,pch=20, col=colfunc(length(lat)), add=FALSE)

	str(tmp)
}

load.lib<-function(libT,l=NULL)
{
	lib.loc <- l
	print(lib.loc)

	if (length(which(installed.packages(lib.loc=lib.loc)[,1]==libT))==0)
	{
		install.packages(libT, lib=lib.loc,repos='http://cran.us.r-project.org')
	}
}

format_alg<-function(i,n)
{
	s<-sprintf("%d",n)
	while(nchar(s)<i)
	{
		s<-sprintf("0%s",s)
	}
	return(s)
}

libraries<-function()
{
	load.lib("RgoogleMaps")
	library(RgoogleMaps)
	load.lib("colorRamps")
	library(colorRamps)
	load.lib("tm")
	library(tm)
	load.lib("chron")
	library(chron)
}

my.substr<-function(ele)
{
	ab<-substr(ele,4,nchar(ele))
	if (substr(ab,1,1)==' ')
	return(substr(ab,3,nchar(ab)))
	return(ab)
}


getlat<-function(ele)
{
	return(as.numeric(unlist(strsplit(ele,"[,]"))[2]))
}

getlon<-function(ele)
{
	return(as.numeric(unlist(strsplit(ele,"[,]"))[1]))
}
# journey-in-data-engineering
A repository containing all the course demo and examples related to journey in the data engineering path

#### Index
 - RatingsCounter
 - FriendsByAge
 - MinTemperature
 - MaxTemperature
 - WordCount
 - WordCountBetter
 - WordCountBetterSorted
 - PopularMovies
 - PopularMoviesNicer


#### Notes
1. Broadcast Variables
```
// loadMovieNames return a Map[Int, String]

// nameDict now stores this on the cluster temporarily to be used later
var nameDict = sc.broadcast(loadMovieNames)

// out of 2 values from x, the 2nd value can be used as a key 
// to the nameDict to fetch the String value from the Map
val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
```


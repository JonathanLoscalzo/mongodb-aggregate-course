
/*Chapter 3: Core Aggregation - Combining Information

Lab - $group and Accumulators
Problem:

In the last lab, we calculated a normalized rating that required us to know what 
the minimum and maximum values for imdb.votes were. These values were found using the $group stage!

For all films that 
won at least 1 Oscar, 
calculate the standard deviation, highest, lowest, and average imdb.rating. 
Use the sample standard deviation expression.

HINT - All movies in the collection that won an Oscar 
begin with a string resembling one of the following in their awards field*/
var x_max = 1521105;
var x_min = 5;
var min = 1;
var max = 10;
var pipeline = [
    {
        $match: {
            awards: {
                $exists: true,
            },
            imdb: {
                $exists: true,
            },
            "imdb.rating": { $exists: true, $gte: 1 },
        }
    },
    {
        $project: {
            awards_oscars: {
                $gte: [{ $size: { $split: ["$awards", "Won"] } }, 2]
            },
            rating: "$imdb.rating"
        },
    },
    {
        $match: {
            awards_oscars: { $eq: true }
        }
    },
    {
        $group: {
            _id: "$awards_oscars",
            deviation: { $stdDevPop: "$rating" },
            samp_deviation: { $stdDevSamp: "$rating" },
            average_rating: { $avg: "$rating" },
            highest_rating: { $max: "$rating" },
            lowest_rating: { $min: "$rating" }
        }
    }
]

db.movies.aggregate([
    {
        $match:
            { awards: { $exists: true } }
    },
    {
        $project:
        {
            awards: 1,
            awards_oscar: { $gte: [{ $size: { $split: ["$awards", "win"] } }, 2] }
        }
    }]).pretty()


//----------------------------------------------------------------------------


/*Chapter 3: Core Aggregation - Combining Information

Lab - $unwind
Problem:

Let's use our increasing knowledge of the Aggregation Framework 
to explore our movies collection in more detail. 
We'd like to calculate how many movies every 
cast member has been in and get an average imdb.rating for each cast member.

What is the name, number of movies, and average rating(truncated to one decimal) 
for the cast member 
    that has been in the most number of movies with English as an available language ?

    Provide the input in the following order and format

{ "_id": "First Last", "numFilms": 1, "average": 1.1 }*/
// TODO: falta un truncate
var pipeline = [
    {
        $match: {
            languages: { $in: ["English"] },
            imdb: {
                $exists: true,
            },
            "imdb.rating": { $exists: true, $gte: 0 },
        }
    }, {
        $unwind: "$cast"
    },
    {
        $group: {
            _id: "$cast",
            numFilms: { "$sum": 1 },
            average: { "$avg": "$imdb.rating" }
        }
    }, {
        $sort: {
            numFilms: -1
        }
    }
]
// ========================
/*
Lab - Using $lookup
Problem:

Which alliance from air_alliances flies the most routes with either 
a Boeing 747 or an Airbus A380 (abbreviated 747 and 380 in air_routes)?

*/

var pipeline = [
    {
        $lookup: {
            from: "air_routes",
            localField: "airlines",
            foreignField: "airline.name",
            as: "routes"
        }
    },
    {
        $addFields: {
            "airplanes": {
                $map: {
                    input: "$routes",
                    as: "route",
                    in: {
                        $split: ["$$route.airplane", " "]
                    }
                }
            }
        }
    },
    {
        $unwind: "$airplanes"
    },
    {
        $unwind: "$airplanes"
    },
    {
        $project: {
            airplanes: 1,
            _id: 1,
            name: 1
        }
    },
    {
        $group: {
            "_id": "$name",
            "airplanes": { $addToSet: "$airplanes" },
            "name": { $first: "$name" }
        }
    },
    // {
    //     $match: {
    //         "airplanes": { $in: ["380", "747"] }
    //     }
    // },
    {
        $addFields: {
            airplanes: {
                $filter: {
                    input: "$airplanes",
                    as: "airplane",
                    cond: { $in: ["$$airplane", ["380", "747"]] }
                }
            }
        }
    }, {
        $project: {
            howMuchRoutes: { $size: "$airplanes" },
            name: 1
        }
    },
    {
        $sort: {
            howMuchRoutes: -1
        }
    }
]

// =================================
/*Determine the approach that satisfies the following question in the most efficient manner:

Find the list of all possible distinct destinations, 
with at most one layover, departing from the base airports of airlines that make part of the "OneWorld" alliance.
The airlines should be national carriers from Germany, Spain or Canada only.
Include both the destination and which airline services that location.
As a small hint, you should find 158 destinations.*/
db.air_alliances.aggregate([{
    $match: { name: "OneWorld" }
}, {
    $graphLookup: {
        startWith: "$airlines",
        from: "air_airlines",
        connectFromField: "name",
        connectToField: "name",
        as: "airlines",
        maxDepth: 0,
        restrictSearchWithMatch: {
            country: { $in: ["Germany", "Spain", "Canada"] }
        }
    }
}, {
    $graphLookup: {
        startWith: "$airlines.base",
        from: "air_routes",
        connectFromField: "dst_airport",
        connectToField: "src_airport",
        as: "connections",
        maxDepth: 1
    }
}, {
    $project: {
        validAirlines: "$airlines.name",
        "connections.dst_airport": 1,
        "connections.airline.name": 1
    }
},
{ $unwind: "$connections" },
{
    $project: {
        isValid: { $in: ["$connections.airline.name", "$validAirlines"] },
        "connections.dst_airport": 1
    }
},
{ $match: { isValid: true } },
{ $group: { _id: "$connections.dst_airport" } }
])
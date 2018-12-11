/*Problem:

How many movies are in both the top ten highest 
rated movies according to the imdb.rating and the metacritic fields? 
We should get these results with exactly one access to the database.

Hint: What is the intersection?*/

var pipeline = [
    {
        "$facet": {
            "metacritic": [
                {
                    $match: {
                        $and: [{ metacritic: { $exists: true } }, { metacritic: { $gte: 0 } }]
                    }
                },
                {
                    "$sort": {
                        metacritic: -1
                    }
                }, {
                    $limit: 20
                }, {
                    $project: {
                        _id: 1,
                        //rating: "$metacritic"
                    }
                }
            ],
            "imdb_rating": [
                {
                    $match: {
                        $and: [
                            { imdb: { $exists: true } },
                            { "imdb.rating": { $exists: true } },
                            { "imdb.rating": { $gt: 0 } }
                        ]
                    }
                }, {
                    "$sort": {
                        "imdb.rating": -1
                    }
                }, {
                    $limit: 20
                }, {
                    $project: {
                        _id: 1,
                        //rating: "$imdb.rating"
                    }
                }
            ],
        },
    }, {
        $project: {
            imdb_rating: 1,
            metacritic: 1,
            commonToBoth: { $setIntersection: ["$imdb_rating", "$metacritic"] }
        }
    }
]
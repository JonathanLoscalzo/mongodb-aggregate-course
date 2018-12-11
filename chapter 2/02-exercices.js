var c = {
    "_id": ObjectId("573a1390f29313caabcd4cf1"),
    "title": "Ingeborg Holm",
    "year": 1913,
    "runtime": 96,
    "released": ISODate("1913-10-27T00:00:00Z"),
    "cast": [
        "Hilda Borgstr�m",
        "Aron Lindgren",
        "Erik Lindholm",
        "Georg Gr�nroos"
    ],
    "poster": "http://ia.media-imdb.com/images/M/MV5BMTI5MjYzMTY3Ml5BMl5BanBnXkFtZTcwMzY1NDE2Mw@@._V1_SX300.jpg",
    "plot": "Ingeborg Holm's husband opens up a grocery store and life is on the sunny side for them and their three children. But her husband becomes sick and dies. Ingeborg tries to keep the store, ...",
    "fullplot": "Ingeborg Holm's husband opens up a grocery store and life is on the sunny side for them and their three children. But her husband becomes sick and dies. Ingeborg tries to keep the store, but because of the lazy, wasteful staff she eventually has to close it. With no money left, she has to move to the poor-house and she is separated from her children. Her children are taken care of by foster-parents, but Ingeborg simply has to get out of the poor-house to see them again...",
    "lastupdated": "2015-08-25 00:11:47.743000000",
    "type": "movie",
    "directors": [
        "Victor Sj�str�m"
    ],
    "writers": [
        "Nils Krok (play)",
        "Victor Sj�str�m"
    ],
    "imdb": {
        "rating": 7,
        "votes": 493,
        "id": 3014
    },
    "countries": [
        "Sweden"
    ],
    "genres": [
        "Drama"
    ]
}


/*Lab: Using Cursor-like Stages
Problem:

MongoDB has another movie night scheduled. 
This time, we polled employees for their favorite actress or actor, and got these results

favorites = [
  "Sandra Bullock",
  "Tom Hanks",
  "Julia Roberts",
  "Kevin Spacey",
  "George Clooney"]
For movies released in the USA with a tomatoes.viewer.rating 
greater than or equal to 3, 
calculate a new field called num_favs 
that represets how many favorites appear in the cast field of the movie.

Sort your results by num_favs, tomatoes.viewer.rating, and title, all in descending order.

What is the title of the 25th film in the aggregation result?*/
var favorites = [
    "Sandra Bullock",
    "Tom Hanks",
    "Julia Roberts",
    "Kevin Spacey",
    "George Clooney"
]
var pipeline = [
    {
        $match: {
            countries: { $in: ["USA"] },
            "tomatoes.viewer.rating": { $gte: 3 },
            cast: { $elemMatch: { $exists: true } },
        }
    },
    {
        $addFields: {
            num_favs: {
                $size: { $setIntersection: ["$cast", favorites] }
            }
        }
    },
    {
        $sort: {
            "num_favs": -1, "tomatoes.viewer.rating": -1, "title": -1,
        }
    },
    { $skip: 24 }, { $limit: 1 }
]

/**
 * Problem:

Calculate an average rating for each movie in our collection 
where English is an available language, 
the minimum imdb.rating is at least 1, 
the minimum imdb.votes is at least 1, 
and it was released in 1990 or after. 
You'll be required to rescale (or normalize) imdb.votes. 
The formula to rescale imdb.votes and calculate normalized_rating 
is included as a handout.

What film has the lowest normalized_rating?
 */
var x_max = 1521105;
var x_min = 5;
var min = 1;
var max = 10;
var pipeline = [
    {
        $match: {
            languages: { $in: ["English"] },
            "imdb": { $exists: true },
            "imdb.rating": { $exists: true, $gte: 1 },
            "imdb.votes": { $exists: true, $gte: 1 },
            released: { $gte: new ISODate("1990-01-01T00:00:00Z") }
        }
    }, {
        $addFields: {
            normalized_rating: {
                $let: {
                    vars: {
                        scaled_votes: {
                            $add: [
                                1,
                                {
                                    $multiply: [
                                        9,
                                        {
                                            $divide: [
                                                {
                                                    $subtract: ["$imdb.votes", x_min]
                                                },
                                                {
                                                    $subtract: [x_max, x_min]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                    },
                    in: { $avg: ["$$scaled_votes", "$imdb.rating"] }
                }
            }
        }
    }, {
        $sort: {
            normalized_rating: 1
        }
    }
]
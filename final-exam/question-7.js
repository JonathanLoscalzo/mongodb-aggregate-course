/*Problem:

Using the air_alliances and air_routes collections, find which alliance has the most unique carriers(airlines) operating between the airports JFK and LHR.

Names are distinct, i.e. Delta != Delta Air Lines

src_airport and dst_airport contain the originating and terminating airport information.*/

/*
- SkyTeam, with 4 carriers
- Star Alliance, with 6 carriers
- OneWorld, with 8 carriers
- OneWorld, with 4 carriers*/

var pipeline = [
    {
        $unwind: "$airlines"
    },

    {
        $lookup: {
            from: "air_airlines",
            localField: "airlines",
            foreignField: "name",
            as: "airline"
        }
    },
    {
        $unwind: "$airline"
    },
    {
        $graphLookup: {
            from: "air_routes",
            startWith: "$airline.name",
            connectFromField: "airline.name",
            connectToField: "airline.name",
            as: "golfers",
            maxDepth: 0,
            restrictSearchWithMatch: { $or: [{ dst_airport: { $eq: "LHR" }, src_airport: { $eq: "JFK" } }, { dst_airport: { $eq: "JFK" }, src_airport: { $eq: "LHR" } }] }
        }
    },
    {
        $addFields: {
            size_golfers: { $size: "$golfers" }
        }
    },
    {
        $match: {
            size_golfers: { $gt: 0 }
        }
    },
    {
        $group: {
            _id: "$name",
            airlines: { $sum: 1 },
            airlines_obj:{$addToSet:"$airlines"},
            routes: { $sum: "$size_golfers" }
        }
    }
]

db.air_alliances.aggregate(pipeline).pretty()
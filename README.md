# Reddit Rank

Creating user graphs to map user influence and subreddit graph to analyze similar subreddits.

## Motivations

The motivation behind this project was to use 2.5 TB of raw JSON data taken from the reddit API and
create two large graphs, one of users and another of subreddits. The users are connected via submissions
and comments. Reddit allows for users to post submissions on different subreddits and it allows for commenters
to post on those submissions. The edges between users are created when one user comments on another users post.
The edge is directed from the commenter to the submitter, this will create a directed graph. The subreddit
graph is different, the edges represent the number of intersecting users. To be more specific, the edges
represent the number of users two subreddits have in common and the weight of the edge is a number value
representing the number of intersecting users. The final product will allow somebody to query a database
and recieve information on user pagerank and similar subreddits.

### Data

The size of the total raw JSON data is 2.5 TB, the data is grouped in monthly batches. There are approximately
600,000 subreddits that exist to date and 234 million unique users every month (2017).  The 2017 year averages
about 40 million user interactions per month and on average since 2006 there have been 22 million user interactions
every month. 

### Installing

A step by step series of examples that tell you have to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc
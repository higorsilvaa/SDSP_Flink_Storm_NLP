package twitter_streaming;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class StreamingJob {
	static StanfordCoreNLP pipeline;
	static Runtime terminal = Runtime.getRuntime();
	static String nameSinkNLP = "NLP_Results";
	static String nameSinkStream = "STREAM_Results";
    public static void main(String[] args) throws Exception {
		// Erase the sink directories to avoid writes conflicts
		terminal.exec("rm -r " + nameSinkNLP + " " + nameSinkStream);

		// Set up pipeline properties
		Properties props = new Properties();
		// Set the list of annotators to run
		props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner");
		// Build pipeline
		pipeline = new StanfordCoreNLP(props);

		// Load and configure the environment
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

		// Define the filters terms
		TweetFilter customFilterInitializer = new TweetFilter();

		// Get keys to authenticate on Twitter
		Properties twitterCredentials = getTwitterCredentials();
		// Get the connection
		TwitterSource twitterSource = new TwitterSource(twitterCredentials);

		// Apply filters terms to the twitter source
		twitterSource.setCustomEndpointInitializer(customFilterInitializer);

		// Define the source of streaming
		DataStream<String> tweetStream = env.addSource(twitterSource);

		// Operators of pipeline
		SingleOutputStreamOperator<Tuple3<Entity,Adjectives,Integer>> flatMap = tweetStream.flatMap(new TweetParser());
		KeyedStream<Tuple3<Entity,Adjectives,Integer>,String> keyBy = flatMap.keyBy(new DefineKey());
		WindowedStream<Tuple3<Entity,Adjectives,Integer>,String,TimeWindow> window = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
		SingleOutputStreamOperator<Tuple3<Entity,Adjectives,Integer>> reduce = window.reduce(new GroupByKey());
		reduce.print();
		reduce.writeAsText(nameSinkNLP);

		// Operators of items counting
		SingleOutputStreamOperator<Tuple2<String,Integer>> map = reduce.map(new ReMap());
		KeyedStream<Tuple2<String,Integer>,String> keyBy2 = map.keyBy(new ReDefineKey());
		WindowedStream<Tuple2<String,Integer>,String,TimeWindow> window2 = keyBy2.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
		SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window2.sum(1);
		sum.print();
		sum.writeAsText(nameSinkStream);

		// Execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	// Map a tweet string in a triple tuple
	public static class TweetParser implements FlatMapFunction<String, Tuple3<Entity,Adjectives,Integer>> {
		@Override
		public void flatMap(String value, Collector<Tuple3<Entity,Adjectives,Integer>> collector) throws Exception {
			// Get the text of tweet, where it comes in a string format
			String text = Tweet.textFromString(value);

			// Verify the validity of text
			if(text.equals("ParserError") || text.equals("LangNotDefined") || text.equals("TwitterOutputError") || text.equals("NotInEnglish")){
				collector.collect(new Tuple3<>(new Entity(text), new Adjectives(new ArrayList<>()), 1));
			} else {
				// Create a document object
				CoreDocument document = new CoreDocument(text);
				// Annotate the document
				pipeline.annotate(document);

				// Get the adjectives and the persons
				List<String> adj = new ArrayList<String>();
				List<String> sub = new ArrayList<String>();
				for(CoreSentence sentence : document.sentences()) {
					List<String> posTags = sentence.posTags();
					List<String> nerTags = sentence.nerTags();
					List<CoreLabel> tokens = sentence.tokens();
					for (int i = 0; i < tokens.size(); i++) {
						if (posTags.get(i).equals("JJ") && removeAccentsANDPunctuation(tokens.get(i).word()).length() > 1)
							adj.add(removeAccentsANDPunctuation(tokens.get(i).word()));
						if (nerTags.get(i).equals("PERSON") && removeAccentsANDPunctuation(tokens.get(i).word()).length() > 1)
							sub.add(removeAccentsANDPunctuation(tokens.get(i).word()));
					}
				}

				// Only if a text has at least one person and one adjective, pass it to collector
				if (sub.size() > 0 && adj.size() > 0)
					for(String pessoa : sub)
						collector.collect(new Tuple3<>(new Entity(pessoa), new Adjectives(adj), 1));
			}
		}
	}

	// Map a triple tuple to a due tuple
	public static class ReMap implements MapFunction<Tuple3<Entity, Adjectives, Integer>, Tuple2<String, Integer>> {
		@Override
		public Tuple2<String, Integer> map(Tuple3<Entity, Adjectives, Integer> entityAdjectivesIntegerTuple3) throws Exception {
			return new Tuple2<>("Total",entityAdjectivesIntegerTuple3.f2);
		}
	}

	// Define the track terms
	public static class TweetFilter implements TwitterSource.EndpointInitializer, Serializable {
		@Override public StreamingEndpoint createEndpoint() {
			StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
			endpoint.trackTerms(Arrays.asList("War", "Putin", "Ukraine", "Russia", "Zelensky", "USA", "United States", "U.S.", "U.S.A."));
			return endpoint;
		}
	}

	// Define a string key in a triple tuple
	public static class DefineKey implements KeySelector<Tuple3<Entity,Adjectives,Integer>,String> {
		@Override
		public String getKey(Tuple3<Entity,Adjectives,Integer> entityAdjectiveTuple2) throws Exception {
			return entityAdjectiveTuple2.f0.getName();
		}
	}

	// Reduce by key, concatenate the adjectives list and some the control integer, where it means the number of joins to a key
	public static class GroupByKey implements ReduceFunction<Tuple3<Entity,Adjectives,Integer>> {
		@Override
		public Tuple3<Entity, Adjectives, Integer> reduce(Tuple3<Entity, Adjectives, Integer> entityAdjectivesTuple2, Tuple3<Entity, Adjectives, Integer> t1) throws Exception {
			entityAdjectivesTuple2.f1.getAdj().addAll(t1.f1.getAdj());
			return new Tuple3<>(entityAdjectivesTuple2.f0, entityAdjectivesTuple2.f1, entityAdjectivesTuple2.f2+t1.f2);
		}
	}

	// Define a string key to a due tuple
	public static class ReDefineKey implements KeySelector<Tuple2<String, Integer>,String> {
		@Override
		public String getKey(Tuple2<String, Integer> entityAdjectiveTuple2) throws Exception {
			return entityAdjectiveTuple2.f0;
		}
	}

	// Configure Twitter source
	private static Properties getTwitterCredentials(){
	    // Put your keys HERE
		String consumer_key = "";
		String consumer_secret = "";
		String token = "";
		String token_secret = "";

		Properties twitterCredentials = new Properties();
		twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, consumer_key);
		twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, consumer_secret);
		twitterCredentials.setProperty(TwitterSource.TOKEN, token);
		twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, token_secret);
		return  twitterCredentials;
	}

	// Normalize a string, removing the punctuation, accents and special symbols
	public static String removeAccentsANDPunctuation(String value) {
		String normalizer = Normalizer.normalize(value, Normalizer.Form.NFD);
		Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
		return pattern.matcher(normalizer).replaceAll("").replaceAll("\\p{Punct}", "").replaceAll("[^a-zA-Z]*", "");
	}
}

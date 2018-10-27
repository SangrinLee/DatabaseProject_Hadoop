import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
public class DatabaseApplication
{
	/* 맵 클래스로, 입력타입은 Text, Text가 되고, 출력타입은 BigData, Text */
	public static class Map extends Mapper<Text, Text, BigData, Text>
	{
		private BigData bigData = new BigData(); // BigData객체 생성(오버헤드를 줄이기 위해 미리 생성)
		
		/* 맵 메소드의 경우 입력되는 키와 밸류의 타입은 Text이고, 각각 문서ID와 문장을 인자로 받게 된다 */
		public void map(Text docID, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString(); // Text 타입의 밸류(문장)의 값을 String타입으로 변환
			StringTokenizer tokenizer = new StringTokenizer(line, "\t\r\n\f |,!#\"$.'%&=+-_^@`~:?<>(){}[];*/"); // StringTokenizer클래스를 통해 line의 문자열을 단어들의 리스트로 변환
			
			while(tokenizer.hasMoreTokens()) // 토큰의 값이 있다면 반복
			{
				bigData.setWord(tokenizer.nextToken().toLowerCase()); // 단어를 소문자로 변경하면서 bigData객체의 word에 저장(맵 처리가 끝난후 이차소팅에 사용됨)
				try 
				{
					bigData.setDocID(Long.parseLong(docID.toString()));
					// Text타입의 밸류를 String타입으로 변환후 Long타입으로 파싱하고, bigData객체의 docID에 저장(맵 처리가 끝난후 이차소팅에 사용됨)
				}
				catch (Exception e) {
					context.getCounter("Error", "DocID conversion error").increment(1); // try문장에서 예외처리 발생시 2개의 인자를 키와 밸류로 출력하고 카운티의 값을 1증가 시킨다
					continue;
				}
				context.write(bigData, docID); // 키를 bigData객체로, 밸류를 docID로 맵을 출력
			}
		}
	} 

	/* 파티션된 데이터를 이차소팅 한뒤 reduce메소드로 작업을 처리하기 위한 리듀스 클래스로, 입력타입은 BigData, Text가 되고 출력타입은 Text, Text */
	public static class Reduce extends Reducer<BigData, Text, Text, Text>
	{
		Comparator<BigData> comparator = new BigDataComparator(); // 우선순위 큐에서 원소들 간의 순서를 정하기 위한 BigDataComparator객체 생성
		PriorityQueue<BigData> queue = new PriorityQueue<BigData>(1, comparator); // 우선순위 큐의 객체를 생성, 첫번째 인자는 큐의 초기크기이고, 두번째 인자는 정렬을 위해 비교할때 사용되는 Comparator인스턴스
		private BigData bigData; // BigData 인스턴스 생성. reduce메소드 내에서 매번 객체를 생성한 후 우선순위 큐에 저장을 해야하므로 미리 생성할 수 없음
		
		/* reduce메소드로의 모든 레코드가 입력이 완료되면 호출되는 메소드로, 우선순위 큐에 있는 객체들을 하나씩 꺼내서 출력하기 위한 메소드 */
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			while(queue.size() != 0) // 우선순위 큐의 사이즈가 0이 아니라면
			{
				long getPrio = queue.size(); // 우선순위 사이즈를 저장
				BigData item = (BigData)queue.remove(); // 큐에서 우선순위가 가장 작은 객체를 빼내서 저장
				item.setPrio(getPrio); // 객체의 우선순위의 값을 저장(값을 빼낼때 남아있는 queue의 사이즈+1이 우선순위가 되므로)
				context.write(new Text(item.showSearch()), new Text(item.getDocIDs())); // Text, Text형식의 레코드를 출력
			}
		}

		/* 이차소팅이 끝난뒤 실행되는 reduce메소드. 입력되는 키와 밸류의 타입은 BigData, Text가 되고 출력은 Text, Text가 된다 */
		public void reduce(BigData key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
				long sum = 0; // 문서 빈도수를 저장하기 위한 변수 초기화
				bigData = new BigData(); // 우선순위 큐에 저장할 bigData객체 생성
				try
				{
					StringBuilder docIDs = new StringBuilder(); // 문서IDs를 출력할 StringBuilder타입의 객체 생성
					boolean first = true; // 문서IDs를 ',문서ID'로 구분하지만  가장 첫번째 문서의 경우 문서ID만 출력하도록 설정하기 위한 변수
					String prevDocID = ""; // 문서ID의 중복을 제거하기 위해 사용하는 String형 변수 초기화
					for (Text val : values) // 밸류로 넘어온 리스트들을 val변수에 저장후 반복
					{
						String curDocID = val.toString(); // Text타입의 val변수를 스트링타입으로 변환
						if (!curDocID.equals(prevDocID)) // 현재문서ID와 이전문서ID가 다르다면
						{
							sum++; // 문서ID의 갯수 증가
							if (!first) // 현재 문서가 가장 첫번째 문서가 아니라면
								docIDs.append(","); // docIDs에 ',' 추가
							else // 현재 문서가 첫번째 문서라면
								first = false; // 첫번째 문서가 아니라는 것을 표시

							docIDs.append(val.toString()); // Text타입의 val을 스트링형으로 변환후 docIDs에 추가
							prevDocID = curDocID; // 이전문서ID에 현재문서ID를 저장(다음 문서와 현재 문서를 비교하기 위해 사용)
						}
					}
					bigData.setWord(key.getWord()); // bigData객체의 word에 key로 받은 word를 저장
					bigData.setDocIDs(docIDs.toString()); // bigData객체의 docIDs에 Text타입의 docIDs를 스트링형으로 변환후 저장
					bigData.setFreq(sum); // bigData객체의 freq에 문서 빈도수 저장
				}
				catch(Exception e)
				{
					context.getCounter("Error", "Reducer Exception:" + key.toString()).increment(1); // try문장에서 예외처리 발생시 2개의 인자를 키와 밸류로 출력하고 카운티의 값을 1증가 시킨다
				}
				queue.add(bigData); // 모든 정보를 저장하고 있는 bigData객체를 우선순위 큐에 추가
			}
	}
	
	/* 우선순위 큐에서 원소들 간의 순서를 정하기 위한 클래스 생성 */
	public static class BigDataComparator implements Comparator<BigData>
	{
		/* Comparator인터페이스의 compare메소드 구현, 비교할 BigData인스턴스를 인자로 받는다  */
		public int compare(BigData x, BigData y)
		{
			if(x.getFreq() < y.getFreq()) // x의 freq가 y의 freq값보다 작다면
				return -1; // -1리턴. 즉 y를 x보다 앞에 넣는다
			if(x.getFreq() > y.getFreq()) // x의 freq가 y의 freq보다 크다면
				return 1; // 1리턴. 즉 x를 y보다 앞에 넣는다
			return 0; // x와 y가 동일하므로 먼저 들어온 순서대로 넣는다
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration(); // Configuration 객체 생성
		Job job = new Job(conf, "Database Application"); // Configuration 인스턴스와 Job의 이름을 인자로 Job객체 생성

		job.setJarByClass(DatabaseApplication.class); // Job인스턴스의 setJarByClass메소드를 호출하여 맵과 리듀스 클래스가 있는 jar이름을 지정
		job.setMapOutputKeyClass(BigData.class); // 맵에서 출력하는 키 타입을 BigData로 지정
		job.setMapOutputValueClass(Text.class); // 맵에서 출력하는 밸류 타입을 Text로 지정
		job.setOutputKeyClass(Text.class); // 리듀스에서 출력하는 키 타입을 Text로 지정
		job.setOutputValueClass(Text.class); // 리듀스에서 출력하는 밸류 타입을 Text로 지정

		job.setMapperClass(Map.class); // 맵 클래스 이름을 이용하여 맵 객체 생성
		job.setReducerClass(Reduce.class); // 리듀스 클래스 이름을 이용하여 리듀스 객체 생성

		job.setPartitionerClass(WordIDPartitioner.class); // 파티셔너 클래스 지정(맵 태스크의 출력 레코드가 생길때마다 파티션 번호에 맞게 구분해주는 역할)
		job.setGroupingComparatorClass(WordIDGroupingComparator.class); // 이차 소팅을 하기 위한 그룹핑 클래스 지정(리듀스 태스크에서 같은 키를 바탕으로 정렬하기 위한 역할), 리듀스 클래스의 reduce메소드가 실행되기 전에 작업을 수행
		job.setSortComparatorClass(WordIDSortComparator.class); // 이차 소팅을 하기 위한 소팅클래스 지정(소팅이 끝난 후 같은 키를 갖는 레코드들을 하나로 묶어서 하나의 리듀스 입력 레코드를 만들고, 밸류를 정렬하여 레코드간의 순서를 정하기 위한 역할), 리듀스 클래스의 reduce메소드가 실행되기 전에 작업을 수행

		job.setInputFormatClass(KeyValueTextInputFormat.class); // 맵으로 입력되는 파일의 포멧을 지정
		job.setOutputFormatClass(TextOutputFormat.class); // 리듀스에서 출력되는 포맷을 지정
		job.setNumReduceTasks(1); // 리듀스 태스크의 숫자를 1로 지정

		FileInputFormat.addInputPath(job, new Path(args[0])); // main함수의 첫번째 인자로 입력파일의 위치 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // main함수의 두번째 인자로 출력파일의 위치 지정

		job.waitForCompletion(true); // 블로킹 함수로 잡이 완전히 종료될때까지 기다림
	}
}


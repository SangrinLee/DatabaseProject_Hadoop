import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

/* 맵 태스크가 새로운 키/밸류 페어를 출력후, 같은 키를 갖는 레코드들을 같은 리듀스 태스크로 보내지도록 하기 위한 Partitioner클래스 */
public class WordIDPartitioner extends Partitioner<BigData, Text> 
{
	protected WordIDPartitioner() {	}

	/* 맵에서 출력 레코드가 생길 때마다 호출(키, 밸류, 리듀스 태스크의 전체 숫자를 인자로 가진다. 즉, 출력 레코드를 어느 리듀스 태스크로 보낼지 결정 */
	public int getPartition(BigData key, Text val, int numPartitions)
	{
		return (key.getWord().hashCode() & Integer.MAX_VALUE) % numPartitions;
		// 키의 해시값을 구한 다음에 리듀스 태스크의 수(main메소드 내에서 Job크래스의 numReduceTasks메소드로 지정)로 나눈 나머지를 리턴, 즉 파티션 번호를 리턴
	}
}

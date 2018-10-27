import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;

/* 같은 리듀서로 모인 레코드들을 소팅하여 같은 키를 갖는 레코드들을 하나의 그룹으로 만들 때 사용하기 위한 클래스 */
public class WordIDGroupingComparator extends WritableComparator
{
	protected WordIDGroupingComparator() 
	{
		super(BigData.class, true);
	}

	/* WritableComparator클래스의 compare메소드를 오버라이딩 */
	public int compare(WritableComparable w1, WritableComparable w2) 
	{
		BigData k1 = (BigData)w1;
		BigData k2 = (BigData)w2;
		return k1.getWord().compareTo(k2.getWord()); // 파티션 데이터에 대해 키를 바탕으로 소팅을 수행하기 때문에 k1의 word와 k2의 word를 비교한 결과를 리턴
	}
}


import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.io.IOException;

/* 같은 키로 묶인 레코드들을 하나의 리듀스 입력 레코드를 만들고, 그룹으로 묶어진 레코드들 간의 순서를 정하기 위한 클래스(밸류를 소팅) */
public class WordIDSortComparator extends WritableComparator
{
	protected WordIDSortComparator() 
	{
		super(BigData.class, true);
	}
	/* WritableComparator클래스의 compare메소드를 오버라이딩, 디폴트로 키값만 비교를 하기때문에 오버라이딩하여 밸류값도 비교를 수행 */
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		BigData k1 = (BigData)w1; 
		BigData k2 = (BigData)w2;
		int result = k1.compareTo(k2); // BigData클래스의 compareTo메소드를 호출하고, compareTo메소드에서는 word를 먼저 비교한후, 두개의 word가 동일한 경우 docID를 비교한 후 결과를 리턴
		return result;
	}
}



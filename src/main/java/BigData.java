import org.apache.hadoop.io.*;

import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.lang.Math;

/* 프로그램에서 사용될 키 타입으로 단어, 이차소팅에 사용할 문서ID, 문서IDs, 문서빈도수, 문서 빈도수순위의 정보를 가지는 클래스 */
public class BigData implements WritableComparable<BigData>
{
	private String word; // 단어를 저장, 이차소팅을 사용할때도 사용됨
	private Long docID; // 이차소팅에 사용할 문서ID
	private String docIDs; // 문서IDs를 저장
	private Long freq; // 문서 빈도수를 저장
	private Long prio; // 문서 빈도수순위를 저장

	/* 역직렬화 될때 호출되는 메소드로 write메소드에서 저장했던 순서 그대로 데이터를 꺼내서 객체의 마지막 상태로 복구하기 위한 메소드 */
	public void readFields(DataInput in) throws IOException {
		word = WritableUtils.readString(in); // 데이터 in을 String형의 데이터 word에 읽고 저장
		docID = in.readLong(); // 데이터 in을 Long형의 데이터 docID에 읽고 저장
	}

	/* 직렬화 될때 호출되는 메소드로 주어진 스트림에 내부 데이터를 차례로 저장하는 일을 수행하기 위한 메소드 */
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, word); // String형의 데이터 word를 out에 쓴다
		out.writeLong(docID); // Long형의 데이터 docID에 out을 쓴다
	}

	/* 호출한 객체와 인자로 받은 객체를 비교하는 메소드 */
	public int compareTo(BigData o) {
		int result = word.compareTo(o.word); // BigData객체의 word를 비교한후 결과를 저장
		if(0 == result) { // 두 객체의 word가 동일하다면
			result = (int)(docID-(o.docID)); // docID를 비교한 후 결과를 저장
		}
		return result; // 결과 리턴
	}


	/* 클래스 인스턴스 변수의 게터메소드와 세터메소드 정의 */
	public String getWord() { return word; }
	public void setWord(String word) { this.word = word; }
	public Long getDocID() { return docID; }
	public void setDocID(Long docID) { this.docID = docID; }
	public Long getFreq() { return freq; }
	public void setFreq(Long freq) { this.freq = freq; }
	public String getDocIDs() { return docIDs; }
	public void setDocIDs(String docIDs) { this.docIDs = docIDs; }
	public Long getPrio() { return prio; }
	public void setPrio(Long prio) { this.prio = prio; }

	/* 리듀스 클래스의 cleanup메소드에서 결과를 출력하기 위한 메소드 */
	public String showSearch() { return word + " (" + freq + ", " + prio + ") " + "-"; }
}


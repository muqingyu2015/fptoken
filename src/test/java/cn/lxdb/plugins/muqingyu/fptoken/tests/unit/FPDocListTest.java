package cn.lxdb.plugins.muqingyu.fptoken.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import cn.lxdb.plugins.muqingyu.fptoken.dataset.common.FPDocList;

class FPDocListTest {

	@Test
	void addDoc_strictlyIncreasing_staysInArrayMode() {
		FPDocList list = new FPDocList(100);
		list.addDoc(1);
		list.addDoc(5);
		list.addDoc(9);
		assertEquals(3, list.docsize());
	}

	@Test
	void addDoc_duplicate_promotesToSparse() throws IOException {
		FPDocList list = new FPDocList(100);
		list.addDoc(1);
		list.addDoc(5);
		list.addDoc(5);
		assertEquals(2, list.docsize());
		List<Integer> seen = new ArrayList<>();
		list.foreach(seen::add);
		seen.sort(Integer::compareTo);
		assertEquals(Arrays.asList(1, 5), seen);
	}

	@Test
	void addAllDocsFrom_mergesArrayIntoSparse() throws IOException {
		FPDocList a = new FPDocList(50);
		a.addDoc(1);
		a.addDoc(2);
		FPDocList b = new FPDocList(50);
		b.addDoc(3);
		b.addDoc(3);
		a.addAllDocsFrom(b);
		assertEquals(3, a.docsize());
	}
}

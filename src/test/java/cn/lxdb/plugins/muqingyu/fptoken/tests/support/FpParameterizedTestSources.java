package cn.lxdb.plugins.muqingyu.fptoken.tests.support;

import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

/** {@link cn.lxdb.plugins.muqingyu.fptoken.tests.unit.FpTokenTermLayoutParameterizedTest} 参数源。 */
public final class FpParameterizedTestSources {

	private FpParameterizedTestSources() {
	}

	public static Stream<Arguments> layoutRoundTripCases() {
		return Stream.of(
				Arguments.of(1, 1, true),
				Arguments.of(1, 1, false),
				Arguments.of(42, 7, true),
				Arguments.of(999, 100, false),
				Arguments.of(3, 512, true));
	}

	public static Stream<Arguments> searchPrefixAlignmentCases() {
		return Stream.of(
				Arguments.of(1, 1),
				Arguments.of(5, 10),
				Arguments.of(100, 200));
	}
}

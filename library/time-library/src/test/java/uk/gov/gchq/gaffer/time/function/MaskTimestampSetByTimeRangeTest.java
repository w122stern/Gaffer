package uk.gov.gchq.gaffer.time.function;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.koryphe.util.TimeUnit;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MaskTimestampSetByTimeRangeTest {

    private Instant instant;
    private MaskTimestampSetByTimeRange maskTimestampSetByTimeRange = new MaskTimestampSetByTimeRange();

    @Before
    public void setup() {
        instant = Instant.now();
    }

    @Test
    public void shouldFilterUsingMillisecondsByDefault() {
        final RBMBackedTimestampSet timestampSet = createTimestampSet();

        maskTimestampSetByTimeRange.setStartTime(instant.plus(Duration.ofDays(100)).toEpochMilli());
        maskTimestampSetByTimeRange.setEndTime(instant.plus(Duration.ofDays(250)).toEpochMilli());

        final RBMBackedTimestampSet expectedTimestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        expectedTimestampSet.add(instant.plus(Duration.ofDays(100L)));
        expectedTimestampSet.add(instant.plus(Duration.ofDays(200L)));

        final RBMBackedTimestampSet actualTimestampSet = maskTimestampSetByTimeRange.apply(timestampSet);

        assertEquals(expectedTimestampSet, actualTimestampSet);
    }

    @Test
    public void shouldBeAbleToChangeTimeUnit() {

        // Given
        final RBMBackedTimestampSet timestampSet = createTimestampSet();

        final long instantInDays = instant.toEpochMilli() / 1000 / 60 / 60 / 24;

        final MaskTimestampSetByTimeRange mask = new MaskTimestampSetByTimeRange.Builder()
                .startTime(instantInDays)
                .endTime(instantInDays + 150L)
                .timeUnit(TimeUnit.DAY)
                .build();

        // When

        final RBMBackedTimestampSet output = mask.apply(timestampSet);

        // Then

        final RBMBackedTimestampSet expected = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.MINUTE)
                .timestamps(Sets.newHashSet(instant, instant.plus(Duration.ofDays(100))))
                .build();

        assertEquals(expected, output);

    }


    @Test
    public void shouldNotMutateOriginalValue() {
        final RBMBackedTimestampSet timestampSet = createTimestampSet();

        maskTimestampSetByTimeRange.setStartTime(instant.plus(Duration.ofDays(100)).toEpochMilli());
        maskTimestampSetByTimeRange.setEndTime(instant.plus(Duration.ofDays(250)).toEpochMilli());

        final RBMBackedTimestampSet actualTimestampSet = maskTimestampSetByTimeRange.apply(timestampSet);

        assertNotEquals(actualTimestampSet, timestampSet);
        assertEquals(4, timestampSet.getTimestamps().size());

    }

    @Test
    public void shouldFilterInclusively() {
        // Given
        final RBMBackedTimestampSet timestampSet = createTimestampSet();

        // When
        maskTimestampSetByTimeRange.setStartTime(instant.plus(Duration.ofDays(100)).toEpochMilli());
        maskTimestampSetByTimeRange.setEndTime(instant.plus(Duration.ofDays(200)).toEpochMilli());

        final RBMBackedTimestampSet actualTimestampSet = maskTimestampSetByTimeRange.apply(timestampSet);

        // Then
        RBMBackedTimestampSet expectedTimestampSet = new RBMBackedTimestampSet.Builder()
                .timeBucket(CommonTimeUtil.TimeBucket.MINUTE)
                .timestamps(Sets.newHashSet(instant.plus(Duration.ofDays(100L)), instant.plus(Duration.ofDays(200L))))
                .build();

        assertEquals(expectedTimestampSet, actualTimestampSet);

    }

    private RBMBackedTimestampSet createTimestampSet() {
        final RBMBackedTimestampSet timestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        timestampSet.add(instant);
        timestampSet.add(instant.plus(Duration.ofDays(100L)));
        timestampSet.add(instant.plus(Duration.ofDays(200L)));
        timestampSet.add(instant.plus(Duration.ofDays(300L)));
        return timestampSet;
    }
}
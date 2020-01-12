#[macro_export]
macro_rules! get_sink_as_mut {
    ($item:expr) => {
        match $item {
            Sink::BaseSink(base) => base,
            Sink::SinkCopy(base) => &mut base.base_sink,
            Sink::SinkPrint(base) => &mut base.base_sink,
            Sink::SinkLimit(base) => &mut base.base_sink,
        }
    };
}

#[macro_export]
macro_rules! get_sink_as_ref {
    ($item:expr) => {
        match $item {
            Sink::BaseSink(base) => base,
            Sink::SinkCopy(base) => &base.base_sink,
            Sink::SinkPrint(base) => &base.base_sink,
            Sink::SinkLimit(base) => &base.base_sink,
        }
    };
}

#[macro_export]
macro_rules! get_ei_as_mut {
    ($item:expr) => {
        match $item {
            EI::Base(base) => base,
            EI::Extend(base) => &mut base.base_ei,
            EI::Intersect(base) => &mut base.base_ei,
        }
    };
}

#[macro_export]
macro_rules! get_ei_as_ref {
    ($item:expr) => {
        match $item {
            EI::Base(base) => base,
            EI::Extend(base) => &base.base_ei,
            EI::Intersect(base) => &base.base_ei,
        }
    };
}

#[macro_export]
macro_rules! get_scan_as_mut {
    ($item:expr) => {
        match $item {
            Scan::Base(base) => base,
            Scan::ScanSampling(base) => &mut base.base_scan,
            Scan::ScanBlocking(base) => &mut base.base_scan,
        }
    };
}

#[macro_export]
macro_rules! get_probe_as_mut {
    ($item:expr) => {
        match $item {
            Probe::BaseProbe(base) => base,
            Probe::PC(base) => &mut base.base_probe,
            Probe::PMV(PMV::BasePMV(base)) => &mut base.base_probe,
            Probe::PMV(PMV::PMVC(pmvc)) => &mut pmvc.base_pmv.base_probe,
        }
    };
}

#[macro_export]
macro_rules! get_scan_as_ref {
    ($item:expr) => {
        match $item {
            Scan::Base(base) => base,
            Scan::ScanSampling(base) => &base.base_scan,
            Scan::ScanBlocking(base) => &base.base_scan,
        }
    };
}

#[macro_export]
macro_rules! get_base_op_as_mut {
    ($item:expr) => {
        match $item {
            Operator::Base(base) => base,
            Operator::Sink(sink) => match sink {
                Sink::BaseSink(base) => &mut base.base_op,
                Sink::SinkCopy(sc) => &mut sc.base_sink.base_op,
                Sink::SinkPrint(sp) => &mut sp.base_sink.base_op,
                Sink::SinkLimit(sl) => &mut sl.base_sink.base_op,
            },
            Operator::Scan(scan) => match scan {
                Scan::Base(base) => &mut base.base_op,
                Scan::ScanSampling(ss) => &mut ss.base_scan.base_op,
                Scan::ScanBlocking(sb) => &mut sb.base_scan.base_op,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base) => &mut base.base_op,
                EI::Extend(base) => &mut base.base_ei.base_op,
                EI::Intersect(base) => &mut base.base_ei.base_op,
            },
            Operator::Build(build) => &mut build.base_op,
            Operator::Probe(probe) => match probe {
                Probe::BaseProbe(base) => &mut base.base_op,
                Probe::PC(pc) => &mut pc.base_probe.base_op,
                Probe::PMV(PMV::BasePMV(base)) => &mut base.base_probe.base_op,
                Probe::PMV(PMV::PMVC(pmvc)) => &mut pmvc.base_pmv.base_probe.base_op,
            },
        }
    };
}

#[macro_export]
macro_rules! get_base_op_as_ref {
    ($item:expr) => {
        match $item {
            Operator::Base(base) => &base,
            Operator::Sink(sink) => match sink {
                Sink::BaseSink(base) => &base.base_op,
                Sink::SinkCopy(sc) => &sc.base_sink.base_op,
                Sink::SinkPrint(sp) => &sp.base_sink.base_op,
                Sink::SinkLimit(sl) => &sl.base_sink.base_op,
            },
            Operator::Scan(scan) => match scan {
                Scan::Base(base) => &base.base_op,
                Scan::ScanSampling(ss) => &ss.base_scan.base_op,
                Scan::ScanBlocking(sb) => &sb.base_scan.base_op,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base) => &base.base_op,
                EI::Extend(base) => &base.base_ei.base_op,
                EI::Intersect(base) => &base.base_ei.base_op,
            },
            Operator::Build(build) => &build.base_op,
            Operator::Probe(probe) => match probe {
                Probe::BaseProbe(base) => &base.base_op,
                Probe::PC(pc) => &pc.base_probe.base_op,
                Probe::PMV(PMV::BasePMV(base)) => &base.base_probe.base_op,
                Probe::PMV(PMV::PMVC(pmvc)) => &pmvc.base_pmv.base_probe.base_op,
            },
        }
    };
}

/// Get common attributes(Origin) from Operator
#[macro_export]
macro_rules! get_op_attr {
    ($item:expr,$name:ident) => {
        match $item {
            Operator::Base(base) => base.$name,
            Operator::Sink(sink) => match sink {
                Sink::BaseSink(base) => base.base_op.$name,
                Sink::SinkCopy(sc) => sc.base_sink.base_op.$name,
                Sink::SinkPrint(sp) => sp.base_sink.base_op.$name,
                Sink::SinkLimit(sl) => sl.base_sink.base_op.$name,
            },
            Operator::Scan(scan) => match scan {
                Scan::Base(base) => base.base_op.$name,
                Scan::ScanSampling(base) => base.base_scan.base_op.$name,
                Scan::ScanBlocking(base) => base.base_scan.base_op.$name,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base) => base.base_op.$name,
                EI::Extend(base) => base.base_ei.base_op.$name,
                EI::Intersect(base) => base.base_ei.base_op.$name,
            },
            Operator::Build(build) => build.base_op.$name,
            Operator::Probe(probe) => match probe {
                Probe::BaseProbe(base) => base.base_op.$name,
                Probe::PC(pc) => pc.base_probe.base_op.$name,
                Probe::PMV(PMV::BasePMV(base)) => base.base_probe.base_op.$name,
                Probe::PMV(PMV::PMVC(pmvc)) => pmvc.base_pmv.base_probe.base_op.$name,
            },
        }
    };
}

/// Get common attributes(reference) from Operator
#[macro_export]
macro_rules! get_op_attr_as_ref {
    ($item:expr,$name:ident) => {
        match $item {
            Operator::Base(base) => &base.$name,
            Operator::Sink(sink) => match sink {
                Sink::BaseSink(base) => &base.base_op.$name,
                Sink::SinkCopy(sc) => &sc.base_sink.base_op.$name,
                Sink::SinkPrint(sp) => &sp.base_sink.base_op.$name,
                Sink::SinkLimit(sl) => &sl.base_sink.base_op.$name,
            },
            Operator::Scan(scan) => match scan {
                Scan::Base(base) => &base.base_op.$name,
                Scan::ScanSampling(base) => &base.base_scan.base_op.$name,
                Scan::ScanBlocking(base) => &base.base_scan.base_op.$name,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base) => &base.base_op.$name,
                EI::Extend(base) => &base.base_ei.base_op.$name,
                EI::Intersect(base) => &base.base_ei.base_op.$name,
            },
            Operator::Build(build) => &build.base_op.$name,
            Operator::Probe(probe) => match probe {
                Probe::BaseProbe(base) => &base.base_op.$name,
                Probe::PC(pc) => &pc.base_probe.base_op.$name,
                Probe::PMV(PMV::BasePMV(base)) => &base.base_probe.base_op.$name,
                Probe::PMV(PMV::PMVC(pmvc)) => &pmvc.base_pmv.base_probe.base_op.$name,
            },
        }
    };
}

/// Get common attributes(mutable) from Operator
#[macro_export]
macro_rules! get_op_attr_as_mut {
    ($item:expr,$name:ident) => {
        match $item {
            Operator::Base(base) => &mut base.$name,
            Operator::Sink(sink) => match sink {
                Sink::BaseSink(base) => &mut base.base_op.$name,
                Sink::SinkCopy(sc) => &mut sc.base_sink.base_op.$name,
                Sink::SinkPrint(sp) => &mut sp.base_sink.base_op.$name,
                Sink::SinkLimit(sl) => &mut sl.base_sink.base_op.$name,
            },
            Operator::Scan(scan) => match scan {
                Scan::Base(base) => &mut base.base_op.$name,
                Scan::ScanSampling(ss) => &mut ss.base_scan.base_op.$name,
                Scan::ScanBlocking(sb) => &mut sb.base_scan.base_op.$name,
            },
            Operator::EI(ei) => match ei {
                EI::Base(base) => &mut base.base_op.$name,
                EI::Extend(base) => &mut base.base_ei.base_op.$name,
                EI::Intersect(base) => &mut base.base_ei.base_op.$name,
            },
            Operator::Build(build) => &mut build.base_op.$name,
            Operator::Probe(probe) => match probe {
                Probe::BaseProbe(base) => &mut base.base_op.$name,
                Probe::PC(pc) => &mut pc.base_probe.base_op.$name,
                Probe::PMV(PMV::BasePMV(base)) => &mut base.base_probe.base_op.$name,
                Probe::PMV(PMV::PMVC(pmvc)) => &mut pmvc.base_pmv.base_probe.base_op.$name,
            },
        }
    };
}
pub mod extend;
pub mod hashjoin;
pub mod operator;
pub mod scan;
pub mod sink;

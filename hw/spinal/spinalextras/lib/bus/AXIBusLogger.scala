package spinalextras.lib.bus

import spinal.core._
import spinal.lib._
import spinal.lib.bus.amba4.axi.Axi4.resp.{DECERR, OKAY, SLVERR}
import spinal.lib.bus.amba4.axi._
import spinal.lib.bus.misc._
import spinalextras.lib.logging.{FlowLogger, SignalLogger}

import scala.language.postfixOps

object AXIBusLogger {
  def decompose(axi : Axi4Bus) = {
    axi match {
      case axi: Axi4 => (axi.name, axi.aw, axi.ar, axi.r, axi.w, axi.b) //.map(x => FlowLogger.asFlow(x))
      case axi: Axi4Shared => {
        val (aw, ar) = (Stream(Axi4Aw(axi.config)), Stream(Axi4Ar(axi.config)))
        aw.payload.assignAllByName(axi.arw.payload)
        ar.payload.assignAllByName(axi.arw.payload)
        aw.valid := axi.arw.valid && axi.arw.write
        ar.valid := axi.arw.valid && ~axi.arw.write
        aw.ready := axi.arw.ready
        ar.ready := axi.arw.ready

        (axi.name, aw, ar, axi.r, axi.w, axi.b) //.map(x => FlowLogger.asFlow(x))
      }
    }

  }

  def stalls(axis: Axi4Bus*): Seq[(Data, Flow[Bits])] = {
    axis.flatMap(axi => {
      val (name, aw, ar, r, w, b) = decompose(axi)
      SignalLogger.concat(s"${name}_signals",
        aw.isStall.setName("aw_stall"),
        ar.isStall.setName("ar_stall"),
        r.isStall.setName("r_stall"),
        w.isStall.setName("w_stall"),
        b.isStall.setName("b_stall"),
        (w.last && w.fire).setName("w_lastFire")
      )
    }
    )
  }

  def flows(addressMapping: AddressMapping, axis: Axi4Bus*): Seq[(Data, Flow[Bits])] = {
    flows(addr =>  {
      if(addressMapping == AllMapping) {
        True
      } else {
        addressMapping.hit(addr)
      }

    }, axis:_*)
  }

  def errors(axis: Axi4Bus*): Seq[(Data, Flow[Bits])] = {
    axis.flatMap(axi => {
      val (_, aw, ar, r, w, b) = decompose(axi)

      // Technically this should track id's but thats expensive
      val errored_aw = Flow(aw.payload)
      errored_aw.payload := RegNextWhen(aw.payload, aw.fire)
      errored_aw.valid := b.resp =/= OKAY && b.fire

      val errored_ar = Flow(ar.payload)
      errored_ar.payload := RegNextWhen(ar.payload, ar.fire)
      errored_ar.valid := r.resp =/= OKAY && r.fire

      Seq(
        errored_aw.setName(aw.name + "Errors"),
        errored_ar.setName(ar.name + "Errors"),
        r.toFlowFire.takeWhen(r.resp =/= OKAY).setName(r.name + "Errors"),
        b.toFlowFire.takeWhen(b.resp =/= OKAY).setName(b.name + "Errors")
      )
    }).map(x => FlowLogger.asFlow(x))
  }

  /** One FlowLogger-sized bus-error record per failed burst (R last / B). */
  def busError(axi: Axi4Bus, masterId: Int, name: String): Flow[BusErrorEvent] = {
    val (_, aw, ar, r, _, b) = decompose(axi)
    val lastAwAddr = RegNextWhen(aw.payload.addr, aw.fire)
    val lastArAddr = RegNextWhen(ar.payload.addr, ar.fire)
    val ev = Flow(BusErrorEvent())
    ev.setName(name)
    ev.valid := False
    ev.payload.assignFromBits(B(0, ev.payload.getBitsWidth bits))

    def causeOf(resp: Bits, data: Bits): UInt = {
      val c = UInt(BusErrorCause.width bits)
      when(resp === DECERR) {
        c := BusErrorCause.DECERR
      } elsewhen (BusErrorSentinel.isLane(data, BusErrorSentinel.UNIMPL)) {
        c := BusErrorCause.UNIMPL
      } elsewhen (BusErrorSentinel.isLane(data, BusErrorSentinel.SLVERR)) {
        c := BusErrorCause.SLVERR
      } elsewhen (BusErrorSentinel.isLane(data, BusErrorSentinel.TIMEOUT_CMD) ||
          BusErrorSentinel.isLane(data, BusErrorSentinel.TIMEOUT_RSP)) {
        c := BusErrorCause.TIMEOUT
      } otherwise {
        c := Mux(resp === SLVERR, BusErrorCause.SLVERR, BusErrorCause.DECERR)
      }
      c
    }

    when(b.fire && b.resp =/= OKAY) {
      val c = Mux(b.resp === DECERR, BusErrorCause.DECERR, BusErrorCause.SLVERR)
      ev.valid := True
      ev.payload.assign(lastAwAddr, True, masterId, c)
    }
    when(r.fire && r.resp =/= OKAY && r.last) {
      ev.valid := True
      ev.payload.assign(lastArAddr, False, masterId, causeOf(r.resp, r.data), r.data.resized)
    }
    ev
  }

  def flows(addressMapping: UInt => Bool, axis: Axi4Bus*): Seq[(Data, Flow[Bits])] = {
    val flows_meta = axis.map(axi => {
      val (_, aw, ar, r, w, b) = decompose(axi)

      val awHit = RegNext(addressMapping(aw.payload.addr))
      val arHit = RegNext(addressMapping(ar.payload.addr))

      val (allowWrite, allowRead) =
          (RegNextWhen(awHit, aw.fire, False),
            RegNextWhen(arHit, ar.fire, False))

      (Seq(
        aw.toFlowFire.stage().takeWhen(awHit).setName(aw.getName()),
        ar.toFlowFire.stage().takeWhen(arHit).setName(ar.getName()),
        r.toFlowFire.stage().takeWhen(allowRead).setName(r.getName()),
        w.toFlowFire.stage().takeWhen(allowWrite).setName(w.getName()),
        b.toFlowFire.stage().takeWhen(allowWrite).setName(b.getName()),
      ).map(x => FlowLogger.asFlow(x)),
        Seq(
          aw.valid,
          ar.valid,
          r.valid,
          w.valid,
          b.valid,
          aw.ready,
          ar.ready,
          r.ready,
          w.ready,
          b.ready
        )
      )

    })

    flows_meta.flatMap(_._1)
  }
  def flows(axis: Axi4Bus*): Seq[(Data, Flow[Bits])] = {
    flows(AllMapping, axis:_*)
  }

  /** AW handshake: ADDR / SIZE / LEN / BURST (named fields for EventLogger decode). */
  def awMeta(axis: Axi4Bus*): Seq[(Data, Flow[Bits])] = {
    axis.flatMap(axi => {
      val (_, aw, _, _, _, _) = decompose(axi)
      val meta = Flow(new Bundle {
        val addr = cloneOf(aw.payload.addr)
        val size = if (aw.payload.size != null) cloneOf(aw.payload.size) else UInt(3 bits)
        val len = if (aw.payload.len != null) cloneOf(aw.payload.len) else UInt(8 bits)
        val burst = if (aw.payload.burst != null) cloneOf(aw.payload.burst) else Bits(2 bits)
      })
      val awName = Option(aw.getName()).filter(_.nonEmpty).getOrElse("aw")
      meta.setName(awName + "_meta")
      meta.valid := aw.fire
      meta.payload.addr := aw.payload.addr
      if (aw.payload.size != null) meta.payload.size := aw.payload.size
      else meta.payload.size := U(log2Up(aw.config.bytePerWord), 3 bits)
      if (aw.payload.len != null) meta.payload.len := aw.payload.len
      else meta.payload.len := 0
      if (aw.payload.burst != null) meta.payload.burst := aw.payload.burst
      else meta.payload.burst := B(1, 2 bits)
      Seq(FlowLogger.asFlow(meta))
    })
  }

  /** W handshake: last AW ADDR/SIZE/LEN/BURST plus this beat's WSTRB (HIP stencil). */
  def writeMeta(axis: Axi4Bus*): Seq[(Data, Flow[Bits])] = {
    axis.flatMap(axi => {
      val (_, aw, _, _, w, _) = decompose(axi)
      val lastAddr = RegNextWhen(aw.payload.addr, aw.fire)
      val lastSize = Reg(UInt(3 bits))
      val lastLen = Reg(UInt(8 bits))
      val lastBurst = Reg(Bits(2 bits))
      val fireSize = if (aw.payload.size != null) aw.payload.size.resize(3 bits) else U(log2Up(aw.config.bytePerWord), 3 bits)
      val fireLen = if (aw.payload.len != null) aw.payload.len.resize(8 bits) else U(0, 8 bits)
      val fireBurst = if (aw.payload.burst != null) aw.payload.burst.resize(2 bits) else B(1, 2 bits)
      when(aw.fire) {
        lastSize := fireSize
        lastLen := fireLen
        lastBurst := fireBurst
      }
      val meta = Flow(new Bundle {
        val addr = cloneOf(aw.payload.addr)
        val size = UInt(3 bits)
        val len = UInt(8 bits)
        val burst = Bits(2 bits)
        val strb = cloneOf(w.payload.strb)
      })
      val awName = Option(aw.getName()).filter(_.nonEmpty).getOrElse("aw")
      meta.setName(awName + "_wmeta")
      meta.valid := w.fire
      // AW+W can fire together; registered last* would then tag this W with the prior AW.
      meta.payload.addr := Mux(aw.fire, aw.payload.addr, lastAddr)
      meta.payload.size := Mux(aw.fire, fireSize, lastSize)
      meta.payload.len := Mux(aw.fire, fireLen, lastLen)
      meta.payload.burst := Mux(aw.fire, fireBurst, lastBurst)
      meta.payload.strb := w.payload.strb
      Seq(FlowLogger.asFlow(meta))
    })
  }
}

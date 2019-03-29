#!python3

import sys, os, time, logging, sqlite3
import types
from send_email import summon_devteam

this_file_dir = os.path.dirname(os.path.abspath(__file__))
method_local_dir = os.path.join(this_file_dir, 'method_local')
containing_dirname = os.path.basename(os.path.dirname(this_file_dir))

from pace_util import (
    pyhamilton, LayoutManager, ResourceType, Plate96, Tip96, LAYFILE,
    HamiltonInterface, ClarioStar, LBPumps, PlateData, Shaker,
    initialize, hepa_on, tip_pick_up, tip_eject, aspirate, dispense, wash_empty_refill,
    tip_pick_up_96, tip_eject_96, aspirate_96, dispense_96,
    resource_list_with_prefix, read_plate, move_plate, add_robot_level_log, add_stderr_logging,
    fileflag, clear_fileflag, run_async, yield_in_chunks, log_banner)

def ensure_meas_table_exists(db_conn):
    '''
    Definitions of the fields in this table:
    lagoon_number - the number of the lagoon, uniquely identifying the experiment, zero-indexed
    filename - absolute path to the file in which this data is housed
    plate_id - ID field given when measurement was requested, should match ID in data file
    timestamp - time at which the measurement was taken
    well - the location in the plate reader plate where this sample was read, e.g. 'B2'
    measurement_delay_time - the time, in minutes, after the sample was pipetted that the
                            measurement was taken. For migration, we consider this to be 0
                            minutes in the absense of pipetting time values
    reading - the raw measured value from the plate reader
    data_type - 'lum' 'abs' or the spectra values for the fluorescence measurement
    '''
    c = db_conn.cursor()
    c.execute('''CREATE TABLE if not exists measurements
                (lagoon_number, filename, plate_id, timestamp, well, measurement_delay_time, reading, data_type)''')
    db_conn.commit()

def db_add_plate_data(plate_data, data_type, plate, vessel_numbers, read_wells):
    db_conn = sqlite3.connect(os.path.join(method_local_dir, containing_dirname + '.db'))
    ensure_meas_table_exists(db_conn)
    c = db_conn.cursor()
    for lagoon_number, read_well in zip(vessel_numbers, read_wells):
        filename = plate_data.path
        plate_id = plate_data.header.plate_ids[0]
        timestamp = plate_data.header.time
        well = plate.position_id(read_well)
        measurement_delay_time = 0.0
        reading = plate_data.value_at(*plate.well_coords(read_well))
        data = (lagoon_number, filename, plate_id, timestamp, well, measurement_delay_time, 
                 reading, data_type)
        c.execute("INSERT INTO measurements VALUES (?,?,?,?,?,?,?,?)", data)
    db_conn.commit()
    db_conn.close()

if __name__ == '__main__':
    local_log_dir = os.path.join(method_local_dir, 'log')
    if not os.path.exists(local_log_dir):
        os.mkdir(local_log_dir)
    main_logfile = os.path.join(local_log_dir, 'main.log')
    logging.basicConfig(filename=main_logfile, level=logging.DEBUG, format='[%(asctime)s] %(name)s %(levelname)s %(message)s')
    add_robot_level_log()
    add_stderr_logging()
    for banner_line in log_banner('Begin execution of ' + __file__):
        logging.info(banner_line)

    num_reader_plates = 5 * 4 # 5 stacks of 4
    num_disp_tip_racks = 6
    num_disp_lagoons = 4
    assert num_disp_lagoons <= 8
    num_lagoons = 8*11 + num_disp_lagoons
    lagoons = range(num_lagoons)
    culture_supply_vol = 50 # mL
    # inducer_vol = 200 # uL
    max_transfer_vol = 985 # uL
    rinse_mix_cycles = 4
    rinse_replacements = 2
    cycle_replace_vol = 333 # uL
    read_sample_vol = 100 # uL
    assert read_sample_vol < cycle_replace_vol
    generation_time = 20 * 60 # seconds
    fixed_lagoon_height = 19 # mm for 500uL lagoons
    lagoon_fly_disp_height = fixed_lagoon_height + 18 # mm
    wash_vol = max_transfer_vol # uL
    DEFAULT_WASTE = 'default_waste'

    lmgr = LayoutManager(LAYFILE)

    lagoon_plate = lmgr.assign_unused_resource(ResourceType(Plate96, 'lagoons'))
    mixing_tips = lmgr.assign_unused_resource(ResourceType(Tip96, 'lagoon_tips'))
    inducer_site = lmgr.assign_unused_resource(ResourceType(Plate96, 'inducer'))
    reader_plate_site = lmgr.assign_unused_resource(ResourceType(Plate96, 'read_plate_site'))
    plate_trash = lmgr.assign_unused_resource(ResourceType(Plate96, 'plate_trash'))
    reader_plates = resource_list_with_prefix(lmgr, 'reader_plate_', Plate96, num_reader_plates, reverse=True)
    culture_reservoir = lmgr.assign_unused_resource(ResourceType(Plate96, 'waffle'))
    culture_tips = lmgr.assign_unused_resource(ResourceType(Tip96, 'culture_tips'))
    disp_tips = resource_list_with_prefix(lmgr, 'disposable_tips_', Tip96, num_disp_tip_racks)
    mixing_corral = lmgr.assign_unused_resource(ResourceType(Tip96, 'lagoon_dirty_tips'))
    reader_tray = lmgr.assign_unused_resource(ResourceType(Plate96, 'reader_tray'))
    temp_layout = lmgr.assign_unused_resource(ResourceType(Tip96, 'temp_tip_layout'))
    bleach_site = lmgr.assign_unused_resource(ResourceType(Tip96, 'RT300_HW_96WashDualChamber1_bleach'))
    rinse_site = lmgr.assign_unused_resource(ResourceType(Tip96, 'RT300_HW_96WashDualChamber1_water'))
    
    sys_state = types.SimpleNamespace()
    sys_state.need_to_refill_washer = True
    sys_state.need_to_read_plate = False
    sys_state.mounted_tips = None

    reader_plate_gen = iter(reader_plates)
    disp_tip_poss = [(temp_layout, i) for i in range(8*11, 8*11+num_disp_lagoons)]

    def next_reader_plate_poss(num_poss=len(lagoons)):
        poss = [None]*num_poss
        for p in range(num_poss):
            try:
                poss[p] = next(reader_plate_pos_gen)
            except StopIteration:
                pass
        return poss

    def disp_tips_gen():
        while True:
            for disp_tip_rack in disp_tips:
                for i in range(0, 96, num_disp_lagoons):
                    yield [(disp_tip_rack, i+j) for j in range(num_disp_lagoons)]
    disp_tips_gen = disp_tips_gen()

    def change_96_tips(ham_int, new_tips): # None is an acceptable argument
        if sys_state.mounted_tips is new_tips or sys_state.mounted_tips == new_tips:
            return
        if sys_state.mounted_tips is not None:
            if sys_state.mounted_tips is DEFAULT_WASTE:
                tip_eject_96(ham_int)
            else:
                tip_eject_96(ham_int, sys_state.mounted_tips)
        sys_state.mounted_tips = new_tips
        if new_tips is not None:
            tip_pick_up_96(ham_int, new_tips)

    def put_96_tips(ham_int, destination, immediately=False):
        if not sys_state.mounted_tips:
            raise RuntimeError('Can\'t put back tips when none are mounted')
        sys_state.mounted_tips = destination
        if immediately:
            change_96_tips(ham_int, None)

    def bleach_mounted_tips(ham_int, destination=None):
        logging.info('\n##### Bleaching currently mounted tips' + (' and depositing at ' + destination.layout_name() if destination else '') + '.')
        if not sys_state.disable_pumps:
            logging.info('\n##### Refilling water and bleach.')
            wash_empty_refill(ham_int, refillAfterEmpty=1, # 1=Refill both chambers
                                       chamber1WashLiquid=1, # 1=liquid 2 (blue container) (water)
                                       chamber2WashLiquid=0) # 0=Liquid 1 (red container) (bleach)
        small_vol = 10
        logging.info('\n##### Bleaching.')
        aspirate_96(ham_int, bleach_site, small_vol, mixCycles=2, mixPosition=1, mixVolume=wash_vol, airTransportRetractDist=30)
        dispense_96(ham_int, bleach_site, small_vol, dispenseMode=9, liquidHeight=10, airTransportRetractDist=30) # mode: blowout
        logging.info('\n##### Rinsing.')
        aspirate_96(ham_int, rinse_site, wash_vol, mixCycles=rinse_mix_cycles, mixPosition=1, mixVolume=wash_vol, airTransportRetractDist=30)
        dispense_96(ham_int, rinse_site, wash_vol, dispenseMode=9, liquidHeight=10, airTransportRetractDist=30) # mode: blowout
        if not sys_state.disable_pumps:
            for i in range(rinse_replacements - 1):
                logging.info('\n##### Refilling water.')
                wash_empty_refill(ham_int, refillAfterEmpty=2, # 2=Refill chamber 1 only
                                           chamber1WashLiquid=1) # 1=liquid 2 (blue container) (water)
                logging.info('\n##### Rinsing.')
                aspirate_96(ham_int, rinse_site, wash_vol, mixCycles=rinse_mix_cycles, mixPosition=1, mixVolume=wash_vol, airTransportRetractDist=30)
                dispense_96(ham_int, rinse_site, wash_vol, dispenseMode=9, liquidHeight=10, airTransportRetractDist=30) # mode: blowout
        if destination:
            put_96_tips(ham_int, destination)
        logging.info('\n##### Done bleaching tips.')

    def reader_plate_id(reader_plate):
        return __file__ + ' plate ' + str(reader_plates.index(reader_plate))

    def clean_reservoir(pump_int, shaker):
        shaker.start(300)
        pump_int.bleach_clean()
        shaker.stop()

    def service_lagoons(ham_int, pump_int, reader_int):
        logging.info('\n\n##### ------------------ Servicing lagoons ------------------')

        logging.info('\n##### Filling reservoir and adding inducer.')
        culture_fill_thread = run_async(lambda: pump_int.refill(culture_supply_vol))
        #while True:
        #    try:
        #        tip_pick_up(ham_int, [next(inducer_tip_pos_gen)])
        #        break
        #    except pyhamilton.NoTipError:
        #        continue
        #liq_class_300uL = 'StandardVolumeFilter_Water_DispenseJet_Empty_with_transport_vol'
        while True:
            try:
                tip_pick_up(ham_int, next(disp_tips_gen))
                break
            except pyhamilton.NoTipError:
                continue
        tip_eject(ham_int, disp_tip_poss)
        culture_fill_thread.join()
        #aspirate(ham_int, [(inducer_site, 0)], [inducer_vol], liquidClass=liq_class_300uL)
        #dispense(ham_int, [(culture_reservoir, 93)], [inducer_vol], liquidClass=liq_class_300uL)
        #tip_eject(ham_int)

        logging.info('\n##### Moving fresh culture into lagoons.')
        change_96_tips(ham_int, culture_tips)
        aspirate_96(ham_int, culture_reservoir, cycle_replace_vol, mixCycles=6, mixVolume=100, liquidHeight=.5, airTransportRetractDist=30)
        waffle_clean_thread = run_async(lambda: (pump_int.empty(culture_supply_vol), clean_reservoir(pump_int, shaker)))
        dispense_96(ham_int, lagoon_plate, cycle_replace_vol, liquidHeight=lagoon_fly_disp_height, dispenseMode=9, airTransportRetractDist=30) # mode: blowout
        put_96_tips(ham_int, culture_tips, immediately=True)

        logging.info('\n##### Mixing lagoons.')
        if sys_state.need_to_read_plate:
            logging.info('\n##### Sampling liquid from lagoons to reader plates.')
            while True:
                try:
                    reader_plate = next(reader_plate_gen)
                    move_plate(ham_int, reader_plate, reader_plate_site)
                    break
                except pyhamilton.LabwareError:
                    pass
            change_96_tips(ham_int, mixing_tips)
            aspirate_96(ham_int, lagoon_plate, read_sample_vol, mixCycles=2, mixPosition=2,
                    mixVolume=400, liquidFollowing=1, liquidHeight=fixed_lagoon_height, airTransportRetractDist=30)
            dispense_96(ham_int, reader_plate_site, read_sample_vol, liquidHeight=5, dispenseMode=9, airTransportRetractDist=30) # mode: blowout
        else:
            change_96_tips(ham_int, mixing_tips)
            aspirate_96(ham_int, lagoon_plate, read_sample_vol, mixCycles=2, mixPosition=2,
                    mixVolume=400, liquidFollowing=1, liquidHeight=fixed_lagoon_height, airTransportRetractDist=30)
            dispense_96(ham_int, lagoon_plate, read_sample_vol, liquidHeight=fixed_lagoon_height+3, dispenseMode=9, airTransportRetractDist=30) # mode: blowout

        logging.info('\n##### Draining lagoons to constant height.')
        excess_vol = max_transfer_vol * .8
        aspirate_96(ham_int, lagoon_plate, excess_vol, liquidHeight=fixed_lagoon_height, airTransportRetractDist=30)
        dispense_96(ham_int, bleach_site, excess_vol, liquidHeight=10, dispenseMode=9, airTransportRetractDist=30) # mode: blowout

        put_96_tips(ham_int, mixing_corral)
        change_96_tips(ham_int, temp_layout)

        if sys_state.need_to_read_plate:
            aspirate_96(ham_int, lagoon_plate, read_sample_vol, mixCycles=2, mixPosition=2,
                    mixVolume=400, liquidFollowing=1, liquidHeight=fixed_lagoon_height, airTransportRetractDist=30)
            dispense_96(ham_int, reader_plate_site, read_sample_vol, liquidHeight=5, dispenseMode=9, airTransportRetractDist=30) # mode: blowout
        else:
            aspirate_96(ham_int, lagoon_plate, read_sample_vol, mixCycles=2, mixPosition=2,
                    mixVolume=400, liquidFollowing=1, liquidHeight=fixed_lagoon_height, airTransportRetractDist=30)
            dispense_96(ham_int, lagoon_plate, read_sample_vol, liquidHeight=fixed_lagoon_height+3, dispenseMode=9, airTransportRetractDist=30) # mode: blowout

        logging.info('\n##### Draining lagoons to constant height.')
        aspirate_96(ham_int, lagoon_plate, excess_vol, liquidHeight=fixed_lagoon_height, airTransportRetractDist=30)
        dispense_96(ham_int, bleach_site, excess_vol, liquidHeight=10, dispenseMode=9, airTransportRetractDist=30) # mode: blowout
        
        put_96_tips(ham_int, DEFAULT_WASTE, immediately=True)

        def bleach():
            change_96_tips(ham_int, mixing_corral)
            bleach_mounted_tips(ham_int, destination=mixing_tips)
            change_96_tips(ham_int, None)

        if sys_state.need_to_read_plate:
            plate_id = reader_plate_id(reader_plate)
            protocols = ['17_8_12_lum', '17_8_12_abs']
            data_types = ['lum', 'abs']
            platedatas = read_plate(ham_int, reader_int, reader_tray, reader_plate_site, protocols,
                    plate_id, plate_destination=plate_trash, async_task=bleach) # throw out plate when done and asynchronously bleach 
            if simulation_on:
                platedatas = [PlateData(os.path.join('assets', 'dummy_platedata.csv'))] * 2 # sim dummies
            for platedata, data_type in zip(platedatas, data_types):
                platedata.wait_for_file()
                db_add_plate_data(platedata, data_type, reader_plate, lagoons, [*range(8*10), *range(8*11, 8*11+num_disp_lagoons)])
            reader_int.plate_in(block=False)
            sys_state.need_to_read_plate = False
        else:
            bleach()
        change_96_tips(ham_int, None)
        waffle_clean_thread.join()
        logging.info('\n##### --------------- Done servicing lagoons ---------------\n')

    def trip_read_plate(*args):
        sys_state.need_to_read_plate = True

    sys_state.disable_pumps = '--no_pumps' in sys.argv
    debug = '--debug' in sys.argv
    simulation_on = debug or '--simulate' in sys.argv
    mid_run = '--continue' in sys.argv
    if mid_run:
        print('CONTINUING A PREVIOUSLY INITIALIZED AND PAUSED RUN. WILL SKIP CLEANING. OK? 5 SECONDS TO CANCEL...')
        time.sleep(5)

    def times_at_intervals(interval, delay=0):
        target_time = time.time() + delay
        while True:
            yield target_time
            target_time += 1 if simulation_on else interval

    schedule_items = [ # tuples (blocking function to schedule, monotonic absolute time generator)
        (trip_read_plate, times_at_intervals(generation_time * 6, delay=-1)), # read plate every few cycles
        (service_lagoons, times_at_intervals(generation_time)),
        ]

    shaker = Shaker()
    with HamiltonInterface(simulate=simulation_on) as ham_int, LBPumps() as pump_int, ClarioStar() as reader_int:
        if sys_state.disable_pumps or simulation_on:
            pump_int.disable()
        if simulation_on:
            reader_int.disable()
            shaker.disable()
        ham_int.set_log_dir(os.path.join(local_log_dir, 'hamilton.log'))
        logging.info('\n##### Priming pump lines and cleaning reservoir.')
        if mid_run:
            prime_and_clean = None
        else:
            prime_and_clean = run_async(lambda: (pump_int.prime(),              # important that the shaker is
                    shaker.start(300), pump_int.bleach_clean(), shaker.stop())) # started and stopped at least once
        initialize(ham_int)
        hepa_on(ham_int, simulate=int(simulation_on))
        logging.info('\n##### Filling bleach so first waste dispense does not froth up.')
        if not sys_state.disable_pumps:
            wash_empty_refill(ham_int, refillAfterEmpty=3, chamber2WashLiquid=0) # 3=chamber 2 only; 0=Liquid 1 (bleach)
        if prime_and_clean:
            prime_and_clean.join()
        try:
            errmsg_str = ''
            start_time = time.time()
            next_times = {}
            while True:
                for task_num, (scheduled_func, interval_gen) in enumerate(schedule_items):
                    if task_num not in next_times:
                        next_times[task_num] = next(interval_gen)
                    if debug or fileflag('debug'):
                        clear_fileflag('debug') if not debug else ''; import pdb; pdb.set_trace()
                    if fileflag('stop'):
                        clear_fileflag('stop'); exit()
                    next_time = next_times[task_num]
                    if time.time() - next_time >= 0 or fileflag('continue'):
                        clear_fileflag('continue')
                        scheduled_func(ham_int, pump_int, reader_int)
                        try:
                            next_times[task_num] = next(interval_gen)
                        except StopIteration:
                            break
                else:
                    time.sleep(.2)
                    continue
                break
        except Exception as e:
            errmsg_str = e.__class__.__name__ + ': ' + str(e).replace('\n', ' ')
            logging.exception(errmsg_str)
            print(errmsg_str)
        finally:
            clear_fileflag('debug')
            shaker.stop()
            if not simulation_on and time.time() - start_time > 3600*2:
                summon_devteam('I\'m concerned that your robot might\'ve stopped. ' + __file__ + ' halted.',
                "Dear DevTeam,\n\nThe method above has stopped executing. This could be a good thing "
                "or a bad thing. Either PACE is done, we're out of reader plates or tips or something, "
                "or SOMEONE MESSED UP.\n\n" +
                ('The following exception message might help you: ' + errmsg_str + '\n\n' if errmsg_str else '') +
                "...If the devteam weren't infallible, I'd have more concerns.\n\nYours in science,\n\n"
                "Hamilton \"Hammeth\" Starlet")
            with LBPumps():
                pass # TODO one last try at shutting pumps down, unsure why that hasn't been working


package org.nla.application.migration.service;

import amberdb.AmberSession;
import lombok.extern.slf4j.Slf4j;
import org.nla.application.migration.repository.AmberRepository;
import org.skife.jdbi.v2.Handle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataCleanUp {

    @Autowired
    AmberRepository repository;

    public void findWorksForCleanUp() {
        log.info("Clean up :: started");
        findWorks();
        log.info("Clean up :: Completed");
    }


    private void findWorks() {
        log.info("findWorks :: started");
        long[] works = new long[]{95, 8780, 25684, 103756, 135109, 135293, 180134, 602679, 614218, 625837, 661283, 683316, 683475, 683907, 687440, 730496, 738292, 745321, 763389, 791670, 801473, 804507, 812563, 817328, 826087, 832679, 832718, 838487, 841908, 914921, 1041498, 1048224, 1168031, 1172634, 1172646, 1275385, 1277020, 1293884, 1328933, 1380968, 1387366, 1560948, 1798786, 1823388, 1950701, 2231552, 2256327, 2257156, 2262115, 2448923, 2531349, 2550929, 2552681, 2554206, 2565912, 2584422, 2611589, 2636893, 2645879, 2811769, 2822538, 3074342, 3149253, 3254727, 3299739, 3375996, 3509163, 3511530, 3522593, 3532031, 3570247, 3572757, 3604420, 3604534, 3604547, 3604560, 3604676, 3604689, 3604750, 3604889, 3604902, 3604915, 3604928, 3604963, 3605092, 3605264, 3608209, 3837864, 3862029, 3891959, 3919099, 3919443, 3919853, 3920013, 3925199, 3925641, 3925763, 3926721, 3934321, 3934399, 4126602, 4129284, 4222766, 4443091, 4486889, 4802664, 4833038, 5270497, 5279232, 5281358, 5282510, 5285265, 5287011, 5287929, 5288127, 5292886, 5292894, 5297402, 5298690, 5348142, 5363104, 5363248, 5419387, 5425785, 5429085, 5445891, 5446530, 5449793, 5450208, 5466385, 5467538, 5475277, 5497895, 5498009, 5500325, 5500413, 5500495, 5500849, 5506026, 5527593, 5859059, 5979624, 6027277, 6027278, 6085261, 6168891, 6173340, 6175487, 6230837, 6241718, 6329317, 6340775, 6796278, 6837546, 6907165, 6907168, 6978301, 6987493, 7067635, 7187195, 7290561, 7351971, 7469225, 7530628, 7587176, 7597991, 7677313, 7707013, 7777384, 7778704, 7779462, 7814471, 7822683, 7822693, 7903743, 8023104, 8030345, 8032062, 8052279, 8127476, 8216660, 8254289, 8259457, 8262841, 8271730, 8353780, 8361790, 8362160, 8363123, 8364136, 8365473, 8366337, 8369687, 8370977, 8371329, 8375421, 8375427, 8376175, 8410402, 8416766, 8421500, 8438308, 8459728, 8464125, 8464615, 8464619, 8464623, 8495233, 8560554, 8623948, 8708853, 8760832, 8822723, 8885045, 8885180, 9141431, 9187702, 10371682, 10372676, 10385502, 10447417, 10470853, 10544089, 10599169, 10659782, 10678872, 10695644, 10697286, 10707869, 11350453, 11371591, 11543101, 11655063, 11781101, 11804233, 11976413, 12042890, 12458625, 12553835, 12813200, 12840263, 13255143, 13269434, 13302911, 13309969, 13311019, 13311385, 13316283, 13327337, 13329159, 13330015, 13331517, 13332021, 13332153, 13332281, 13333149, 13333211, 13347131, 13350188, 13350498, 13353977, 13354612, 13376894, 13378192, 13378196, 13382126, 13382386, 13383768, 13383769, 13383770, 13383771, 13383772, 13383773, 13383774, 13383775, 13383936, 13384561, 13385265, 13385586, 13385596, 13386144, 13387312, 13387859, 13430639, 13478774, 13478892, 13506181, 13516105, 13529171, 13531639, 13556762, 13571716, 13612753, 13681739, 13771576, 13775276, 13776898, 13867833, 13871966, 13885578, 13930509, 13932385, 13945476, 13945894, 13949560, 14017535, 14023437, 14037765, 14039213, 14039259, 14040780, 14041410, 14062585, 14094563, 14096026, 14096075, 14096120, 14096173, 14096234, 14096279, 14096324, 14096373, 14096418, 14096463, 14096519, 14096564, 14096611, 14096656, 14096716, 14096850, 14096909, 14097027, 14097320, 14097568, 14097746, 14098015, 14098233, 14098437, 14098647, 14098760, 14098798, 14098833, 14098881, 14098929, 14098978, 14099034, 14099114, 14099169, 14099264, 14099373, 14099474, 14099569, 14099680, 14099766, 14099853, 14099908, 14099975, 14101156, 14101320, 14101355, 14101404, 14120416, 14133882, 14154695, 14158041, 14161438, 14182429, 14182695, 14185503, 14248560, 14249305, 14249385, 14249485, 14249586, 14250130, 14250367, 14286039, 14291280, 14292443, 14343918, 14508078, 14553354, 14586678, 14596824, 14676290, 14738318, 14795657, 14836054, 14898921, 14899118, 14966900, 14994082, 15007904, 15238600, 15278434, 15278895, 15282840, 15296397, 15304294, 15308366, 15309036, 15327652, 15333555, 15342244, 15344310, 15346556, 15347256, 15348897, 15349615, 15351808, 15351878, 15351946, 15359021, 16384289, 16423952, 16424967, 16426314, 16427838, 16428847, 16429406, 16429995, 16430589, 18202602, 18281336, 18281338, 18349825, 18394556, 18394583, 18401391, 18406769, 18424028, 18452966, 18462166, 18462204, 18464426, 18469850, 18495647, 18517532, 18518224, 18596048, 18621425, 18731551, 18732282, 18733071, 18752123, 18767936, 18769320, 18770342, 18770877, 18788352, 18796596, 18796608, 18932818, 18940975, 18944792, 18974065, 19047061, 19077902, 19080355, 19080433, 19282557, 19373904, 19411810, 19484550, 19549358, 19555006, 19587243, 19609047, 19619600, 19684210, 19892358, 19905739, 19954808, 19969952, 19994401, 19995180, 19996055, 20013226, 20050697, 20083414, 20111120, 20249368, 20249369, 20249377, 20249378, 20249386, 20249387, 20249395, 20249396, 20249404, 20249405, 20249420, 20249421, 20249429, 20249430, 20249438, 20249439, 20249447, 20249448, 20249456, 20249457, 20279694, 20408678, 20683937, 20752358, 20810154, 20810220, 20810286, 20810353, 20810419, 20810485, 20810551, 20810617, 20810683, 20810749, 20810815, 20810881, 20810947, 20859895, 20866507, 20866521, 20870568, 21119464, 21207792, 21219520, 21229352, 21237079, 21258464, 21268016, 21295824, 21297466, 21325530, 21343785, 21343787, 21343793, 21343894, 21344149, 21344151, 21353770, 21366366, 21369365, 21511842, 21577159, 21663046, 21742201, 21892550, 21913916, 22050817, 22078438, 22089974, 22093796, 22153410, 22187785, 22191412, 22237188, 22278403, 22292720, 22386578, 22559843, 22619011, 22673774, 22961839, 23003943, 23004138, 23030947, 23065559, 23129986, 23150737, 23216087, 23217612, 23260662, 23269032, 23274221, 23283887, 23283911, 23289926, 23291162, 23325095, 23331533, 23334540, 23354060, 23372464, 23379396, 23379887, 23395912, 23396512, 23403271, 23405818, 23406287, 23420876, 23421651, 23427062, 23427096, 23427418, 23428147, 23428246, 23428555, 23430036, 23433911, 23441150, 23442862, 23443826, 23443848, 23457153, 23457205, 23464113, 23464208, 23464237, 23464592, 23465319, 23465612, 23466351, 23466435, 23467129, 23470425, 23471370, 23488134, 23505322, 23511645, 23609482, 23620101, 23698637, 23710501, 23721667, 23721679, 23736890, 23741369, 23741650, 23741651, 23741657, 23741891, 23741892, 23741893, 23741894, 23746524, 23746779, 23746780, 23746781, 23746828, 23746829, 23746831, 23747902, 23755589, 23830945, 23960032, 23964640, 23996873, 23996941, 24015640, 24015689, 24047801, 24047919, 24048041, 24087405, 24093215, 24094059, 24110853, 24111015, 24129189, 24145073, 24145408, 24145422, 24145424, 24145428, 24145436, 24145438, 24145440, 24145442, 24145444, 24145448, 24145450, 24145454, 24152718, 24186999, 24189500, 24197361, 24206894, 24291169, 24305255, 24322728, 24326569, 24327513, 24327816, 24338146, 24338147, 24355586, 24355769, 24367182, 24411936, 24411976, 24412035, 24413894, 24463036, 24463061, 24463137, 24482495, 24518238, 24538399, 24539578, 24548424, 24551457, 24555225, 24555263, 24597075, 24812926, 24817746, 24820166, 24820337, 24823205, 24898253, 25028562, 25081878, 25330233, 25400537, 25471233, 25494231, 25538032, 25555691, 25555982, 25563965, 25583254, 25596753, 25598554, 25605728, 25682722, 25683444, 25683464, 25702981, 25713028, 25715731, 25721432, 25739618, 25740536, 25779412, 25848881, 25978107, 25978156, 25980069, 25980919, 25986078, 25986156, 25986588, 25996998, 25997274, 25998740, 25998860, 25998883, 25999079, 25999201, 25999231, 25999233, 25999235, 25999237, 25999239, 25999241, 25999243, 25999245, 25999247, 25999249, 25999251, 25999253, 25999255, 25999257, 26001070, 26070189, 26073200, 26218745, 26239911, 26273086, 26320406, 26342951, 26344207, 26352041, 26391688, 26395084, 26548802, 26607841, 26639880, 26658138, 26676774, 26678652, 26695600, 26726528, 26731532, 26736221, 26745154, 26746763, 26750425, 26751666, 26755233, 26757894, 26760825, 26795227, 26812899, 26820412, 26835418, 26835775, 26872533, 26878403, 26879480, 26880500, 26895867, 26895900, 26914960, 27045265, 27061088, 27085250, 27085494, 27089150, 27432218, 27438835, 27463072, 27469245, 27481161, 27481395, 27487458, 27491326, 27538968, 27558011, 27582525, 27582527, 27582611, 27582613, 27582619, 27582655, 27590425, 27595435, 27703498, 27714775, 27820437, 27824450, 27837288, 27855416, 27857654, 27861501, 27864163, 27864173, 27867892, 27868212, 27963301, 27973275, 28003938, 28168706, 28177781, 28178551, 28239063, 28242759, 28270276, 28278533, 28317559, 28343429, 28343433, 28343435, 28343437, 28343439, 28343443, 28343445, 28343451, 28343453, 28343455, 28343459, 28343461, 28343463, 28343465, 28343467, 28343469, 28343475, 28343477, 28343479, 28343481, 28343483, 28343485, 28343487, 28343489, 28343491, 28343493, 28343495, 28343497, 28343499, 28343501, 28343503, 28343505, 28343507, 28343509, 28343511, 28343513, 28343515, 28347567, 28367304, 28396573, 28396574, 28400274, 28410776, 28420545, 28422397, 28426536, 28428398, 28445418, 28522107, 28522135, 28524299, 28528185, 28562322, 28576842, 28581278, 28605482, 28609831, 28626515, 28634879, 28637446, 28651900, 28651914, 28651916, 28651918, 28651922, 28651940, 28661837, 28737376, 28742213, 28761228, 28763481, 28765759, 28774870, 28798710, 28798712, 28798714, 28798716, 28798718, 28798720, 28798722, 28798724, 28798726, 28798728, 28798730, 28798732, 28798734, 28798736, 28798738, 28798740, 28798742, 28834327, 28838973, 28855475, 28855686, 28898652, 28898653, 28898905, 28898908, 28898909, 28898910, 28898912, 28898913, 28899301, 28899302, 28901661, 29017619, 29020781, 29020783, 29020785, 29020787, 29020789, 29020791, 29020793, 29020795, 29020797, 29020799, 29020801, 29020803, 29020805, 29020807, 29020809, 29020888, 29024479, 29142353, 29142393, 29142412, 29332406, 29499001, 29642754, 29876231, 29907105, 29919205, 29920732, 29951311, 29982524, 30008376, 30032704, 30048082, 30135364, 30135426, 30136273, 30140864, 30148386, 30149262, 30149420, 30154458, 30154995, 30158452, 30210689, 30219819, 30257282, 30260122, 30266612, 30273739, 30301441, 30369372, 30375012, 30375337, 30409706, 30436112, 30436655, 30437238, 30438513, 30491098, 30493036, 30493104, 30493158, 30508735, 30530615, 30555084, 30557974, 30558630, 30558784, 30572134, 30574918, 30581992, 30662055, 30711817, 30731151, 30851947, 30860462, 30898827, 31031497, 31038845, 31045888, 31126739, 31128197, 31128503, 31192248, 31273071, 31290244, 31323073, 31451409, 31452070, 31459203, 31459375, 31463136, 31464400, 31497160, 31597134, 31598334, 31598394, 31598409, 31598412, 31613341, 31641559, 31642366, 31649718, 31745241, 31758166, 31883874, 31902640, 31904250, 31925589, 31933683, 31934239, 31941102, 31941261, 31942009, 31942087, 31942175, 31960386, 31968781, 31969016, 31969057, 31970729, 31971406, 31972758, 31972781, 31973912, 31973975, 31973992, 31974176, 31974202, 31974222, 31974240, 31974274, 31974291, 31974310, 31974328, 31974339, 31974377, 31975570, 31975920, 31976869, 31976880, 32023044, 32024905, 32027556, 32040396, 32055225, 32059388, 32059428, 32079031, 32084437, 32087536, 32088583, 32088942, 32090387, 32091313, 32094169, 32095297, 32096059, 32096842, 32097991, 32102495, 32103299, 32112605, 32116780, 32116990, 32117009, 32197897, 32265617, 32265656, 32267292, 32276020, 32277021, 32278100, 32405920, 32423515, 32424790, 32425059, 32425093, 32448587, 32450287, 32450352, 32450365, 32450797, 32451358, 32451529, 32457252, 32462822, 32483953, 32484980, 32486664, 32488398, 32495279, 32524181, 32564453, 32584717, 32599762, 32599772, 32599782, 32599794, 32599804, 32599813, 32599833, 32599843, 32607590, 32655530, 32701681, 32705263, 32771428, 32780793, 32792027, 33019189, 33020511, 33021867, 33035917, 33165627, 33204215, 33223621, 33366255, 33557328, 33673735, 33685930, 33693225, 33694081, 33695085, 33793755, 33798436, 33799945, 33961683, 34064920, 34112872, 34133286, 34196208, 34225812, 34323122, 34365852, 34376873, 34389260, 34631348, 34673692, 35021881, 35030863, 35115962, 35117659, 35129856, 35192917, 35192920, 35208209, 35256021, 35288081, 35297828, 35337392, 35378040, 35390879, 35518300, 35524396, 35546050, 35562753, 35588124, 35698817, 35724123, 35774707, 35847245, 35878226, 35889225, 35899298, 35936968, 35938666, 35938746, 35938858, 35939025, 35940561, 35949990, 35951169, 35951264, 35952419, 35956395, 35956419, 35972522, 35976210, 36033132, 36040976, 36081726, 36081821, 36115985, 36134728, 36150150, 36208426, 36209281, 36209474, 36225602, 36226451, 36233079, 36265636, 36269697, 36271591, 36289999, 36295881, 36323666, 36334265, 36336420, 36336809, 36342538, 36346103, 36360961, 36366566, 36409403, 36450304, 36454476, 36455062, 36455175, 36455292, 36465405, 36465540, 36474382, 36489207, 36495539, 36497482, 36497507, 36507547, 36528857, 36529765, 36569708, 36580903, 36592660, 36612007, 36612367, 36622628, 36631893, 36642834, 36649186, 36649406, 36664326, 36666112, 36674896, 36800038, 36824252, 36829738, 36859285, 36900571, 36901566, 36903208, 36903277, 36954315, 36971966, 36977137, 36978645, 36985524, 37044302, 37049406, 37049465, 37064216, 37120451, 37120710, 37120821, 37187398, 37202490, 37206729, 37216157, 37218237, 37218385, 37220914, 37256158, 37335212, 37468172, 37502371, 37595834, 37600565, 37607729, 37611495, 37613053, 37626351, 37627668, 37648369, 37666870, 37669747, 37671977, 37672123, 37672438, 37672821, 37673065, 37673556, 37673811, 37675722, 37691229, 37691657, 37691876, 37692129, 37692531, 37692737, 37692948, 37693147, 37699082, 37707866, 37707963, 37712589, 37712746, 37712898, 37713025, 37713988, 37714109, 37714242, 37714862, 37715643, 37715801, 37715958, 37716479, 37716643, 37716824, 37722833, 37723102, 37736319, 37738401, 37739450, 37758911, 37777939, 37780000, 37780098, 37780433, 37780536, 37780633, 37780858, 37789626, 37790657, 37797901, 37800880, 37805197, 37810481, 37818135, 37818415, 37862284, 37863249, 37865048, 37920238, 37943877, 37977467, 37978114, 37988491, 37988631, 37990778, 37991107, 37991338, 37991696, 37992005, 38001053, 38043031, 38047684, 38076318, 38081076, 38126437, 38131048, 38131379, 38135443, 38136306, 38154274, 38162737, 38172002, 38235329, 38235331, 38235333, 38235335, 38235337, 38235339, 38235341, 38235343, 38235345, 38235347, 38235349, 38235353, 38235355, 38235357, 38235359, 38235361, 38235367, 38235369, 38235371, 38235373, 38235375, 38235377, 38235379, 38235381, 38235383, 38235385, 38235387, 38235389, 38235391, 38235393, 38235395, 38235397, 38235399, 38235425, 38235439, 38235445, 38235447, 38267724, 38281859, 38301675, 38311034, 38323007, 38323245, 38530977, 38531697, 38535742, 38541018, 38545985, 38603512, 38754762, 38755385, 38755453, 38801161, 38851525, 38860665, 38895711, 38978574, 39085972, 39100791, 39101855, 39110004, 39150228, 39154195, 39222473, 39223941, 39225227, 39227048, 39243468, 39252765, 39262945, 39283521, 39289278, 39291131, 39291256, 39297517, 39309838, 39314468, 39360402, 39367058, 39375528, 39379453, 39380529, 39383591, 39399157, 39406654, 39408111, 39408985, 39409809, 39413802, 39414021, 39414432, 39428877, 39428932, 39429009, 39432885, 39432919, 39486552, 39500084, 39524130, 39528111, 39555377, 39559400, 39561864, 39565269, 39588739, 39607665, 39638545, 39639273, 39655248, 39685119, 39685207, 39729099, 39786725, 39816010, 39821447, 39828981, 39855738, 39872192, 39901262, 39915102, 39972601, 40020581, 40022583, 40022978, 40030930, 40040396, 40040401, 40040406, 40112314, 40160588, 40180287, 40180480, 40180682, 40180885, 40181087, 40181289, 40181740, 40182364, 40182566, 40226082};
        try (AmberSession amberSession = repository.getAmberDb().begin()) {
            try (Handle amberHandle = repository.getAmberDBI()) {
               amberHandle.select("SELECT DISTINCT e.v_in, e.v_out, e.edge_order\n" +
                       "FROM node v,\n" +
                       "\tflatedge e,\n" +
                       "\twork p\n" +
                       "WHERE e.v_in = '37669747'\n" +
                       "  AND e.v_out = v.id\n" +
                       "  AND e.label = 'isPartOf'\n" +
                       "  AND p.id = v.id\n" +
                       "  AND p.type IN ('Work' , 'Page', 'EADWork')\n" +
                       "ORDER BY e.edge_order;");

            }
        } finally {
            log.info("findWorks :: completed");
        }
    }

}